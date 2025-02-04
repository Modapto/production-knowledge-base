# Elastic stack (ELK) on Docker

[![Elastic Stack version](https://img.shields.io/badge/Elastic%20Stack-8.15.1-00bfb3?style=flat&logo=elastic-stack)](https://www.elastic.co/blog/category/releases)
[![Build Status](https://github.com/deviantony/docker-elk/workflows/CI/badge.svg?branch=main)](https://github.com/deviantony/docker-elk/actions?query=workflow%3ACI+branch%3Amain)

Run with Docker and Docker Compose.

It gives you the ability to analyze any data set by using the searching/aggregation capabilities of Elasticsearch and
the visualization power of Kibana.

Based on the [official Docker images][elastic-docker] from Elastic:

* [Elasticsearch](https://github.com/elastic/elasticsearch/tree/main/distribution/docker)
* [Logstash](https://github.com/elastic/logstash/tree/main/docker)
* [Kibana](https://github.com/elastic/kibana/tree/main/src/dev/build/tasks/os_packages/docker_generator)

Other available stack variants:

* [`tls`](https://github.com/deviantony/docker-elk/tree/tls): TLS encryption enabled in Elasticsearch, Kibana (opt in),
  and Fleet
* [`searchguard`](https://github.com/deviantony/docker-elk/tree/searchguard): Search Guard support

---
# Setup the PKB 

## tl;dr

```sh
docker-compose up setup
```

```sh
docker-compose up
```
---

## Requirements

### Host setup

* [Docker Engine][docker-install] version **18.06.0** or newer
* [Docker Compose][compose-install] version **1.28.0** or newer (including [Compose V2][compose-v2])
* 2 GB of RAM

> [!NOTE]
> Especially on Linux, make sure your user has the [required permissions][linux-postinstall] to interact with the Docker
> daemon.

By default, the stack exposes the following ports:

* 9600: Logstash monitoring API
* 9200: Elasticsearch HTTP
* 9300: Elasticsearch TCP transport
* 5601: Kibana
* 5044: Logstash Beats input
* 50000: Logstash TCP input

## Usage

Give Kibana about a minute to initialize, then access the Kibana web UI by opening <http://localhost:5601> in a web
browser.

---

## Deployed PKB on ATC cloud

Since the latest version is deployed on the cloud you can access <https://kibana.modapto.atc.gr/app/security/dashboards> in a web browser and use the credentials above to log in.
---

## License

This project has received funding from the European Union's Horizon 2022 research and innovation programm, under Grant Agreement 101091996.

For more details about the licence, see the [LICENSE](LICENSE) file.

## Contributors

- Stella Markopoulou (<s.markopoulou@aegisresearch.eu>)
