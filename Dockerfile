# Hybrid Orchestrator v5.0 - Dockerfile
FROM python:3.11-slim

LABEL maintainer="orchestrator"
LABEL version="5.0"
LABEL description="Multi-Agent Hybrid Orchestrator with Hermes and OpenClaw"

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY orchestrator_v5.py .

# Create necessary directories
RUN mkdir -p /app/tasks /app/logs /app/state /app/cache

# Environment variables
ENV PYTHONUNBUFFERED=1
ENV ORCHESTRATOR_VERSION=5.0

# Expose API port
EXPOSE 5000

# Default command - start API server
CMD ["python", "orchestrator_v5.py", "/api-server", "5000"]

# Alternative commands
# CLI mode: python orchestrator_v5.py /status
# API mode: python orchestrator_v5.py /api-server 5000
