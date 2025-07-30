# Base Image for Elasticsearch
FROM ghcr.io/modapto/modapto-pkb-elasticsearch:0.1 as elasticsearch

# Set Elasticsearch environment variables
ENV discovery.type=single-node \
    ES_JAVA_OPTS="-Xms512m -Xmx512m" \
    ELASTIC_PASSWORD=${ELASTIC_PASSWORD:-}

# Copy Elasticsearch configuration
COPY ./elasticsearch/config/elasticsearch.yml /usr/share/elasticsearch/config/elasticsearch.yml


# Base Image for Logstash
FROM ghcr.io/modapto/modapto-pkb-logstash:0.1 as logstash

# Set Logstash environment variables
ENV LS_JAVA_OPTS="-Xms256m -Xmx256m" \
    LOGSTASH_INTERNAL_PASSWORD=${LOGSTASH_INTERNAL_PASSWORD:-}

# Copy Logstash configuration and pipeline
COPY ./logstash/config/logstash.yml /usr/share/logstash/config/logstash.yml
COPY ./logstash/pipeline /usr/share/logstash/pipeline


# Base Image for Kibana
FROM ghcr.io/modapto/modapto-pkb-kibana:0.1 as kibana

# Set Kibana environment variables
ENV KIBANA_SYSTEM_PASSWORD=${KIBANA_SYSTEM_PASSWORD:-}

# Copy Kibana configuration
COPY ./kibana/config/kibana.yml /usr/share/kibana/config/kibana.yml


# Final Image Combining ELK Stack
FROM ubuntu:22.04

# Install necessary tools
RUN apt-get update && \
    apt-get install -y curl openjdk-11-jdk && \
    apt-get clean

# Copy Elasticsearch, Logstash, and Kibana from previous stages
COPY --from=elasticsearch /usr/share/elasticsearch /usr/share/elasticsearch
COPY --from=logstash /usr/share/logstash /usr/share/logstash
COPY --from=kibana /usr/share/kibana /usr/share/kibana

# Copy setup scripts
COPY ./setup/entrypoint.sh /entrypoint.sh
COPY ./setup/lib.sh /lib.sh
COPY ./setup/roles /roles

# Make entrypoint executable
RUN chmod +x /entrypoint.sh

# Expose necessary ports
EXPOSE 9200 9300 5044 50000 9600 5601

# Set environment variables for the setup process
ENV ELASTIC_PASSWORD=${ELASTIC_PASSWORD:-} \
    LOGSTASH_INTERNAL_PASSWORD=${LOGSTASH_INTERNAL_PASSWORD:-} \
    KIBANA_SYSTEM_PASSWORD=${KIBANA_SYSTEM_PASSWORD:-} \
    METRICBEAT_INTERNAL_PASSWORD=${METRICBEAT_INTERNAL_PASSWORD:-} \
    FILEBEAT_INTERNAL_PASSWORD=${FILEBEAT_INTERNAL_PASSWORD:-} \
    HEARTBEAT_INTERNAL_PASSWORD=${HEARTBEAT_INTERNAL_PASSWORD:-} \
    MONITORING_INTERNAL_PASSWORD=${MONITORING_INTERNAL_PASSWORD:-} \
    BEATS_SYSTEM_PASSWORD=${BEATS_SYSTEM_PASSWORD:-}

# Install Python and dependencies
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    pip3 install kafka-python elasticsearch && \
    apt-get clean

# Copy the Python script
COPY ./filter_modules.py /filter_modules.py

# Modify CMD to run ELK services + Python script
CMD /entrypoint.sh & \
    /usr/share/elasticsearch/bin/elasticsearch & \
    /usr/share/logstash/bin/logstash & \
    /usr/share/kibana/bin/kibana & \
    python3 /filter_modules.py & \
    tail -f /dev/null
