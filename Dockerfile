FROM python:3.9-slim

WORKDIR /app

# Install dependencies first to leverage Docker cache
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to cache dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY custom_components/ /app/custom_components/
COPY aux_cloud_mqtt.py /app/

# Set up entrypoint and healthcheck
# HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
#     CMD python3 -c "import paho.mqtt.client as mqtt; client = mqtt.Client(); client.connect('localhost', 1883, 5); client.disconnect()"

ENTRYPOINT ["python3", "/app/aux_cloud_mqtt.py"]
