FROM python:3.11-slim

WORKDIR /app

# dependencies and script
COPY script.py .
COPY requirements.txt .

# Εγκατάσταση dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Εκτέλεση του script
CMD ["python", "-u", "script.py"]
