# Use official Python slim image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Set working directory
WORKDIR /app

# Install system dependencies in a single step to avoid cache issues
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
        ca-certificates \
        build-essential \
        apt-transport-https \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# Copy the entire API folder
COPY . /app/api

# Set working directory to API folder
WORKDIR /app/api

# Expose the application port
EXPOSE 5000

# Run the application with Gunicorn
CMD ["gunicorn", "api.app:app", "-b", "0.0.0.0:5000", "--workers=4"]
