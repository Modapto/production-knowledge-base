FROM python:3.11-slim

WORKDIR /app

# dependencies and script
COPY script.py .
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose FastAPI port
EXPOSE 5000

# Run the worker (API + Kafka, ανάλογα με το RUN_MODE)
CMD ["python", "-u", "script.py"]
