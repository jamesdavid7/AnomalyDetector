# Use slim Python image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app

# Install only required system dependencies for psycopg2-binary and HTTPS
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libpq-dev \
        ca-certificates \
        apt-transport-https \
        wget \
    && rm -rf /var/lib/apt/lists/*

# Copy Python requirements and install
COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# Copy entire API folder
COPY . /app/api
WORKDIR /app/api

EXPOSE 5000

# Run the application with Gunicorn
CMD ["gunicorn", "api.app:app", "-b", "0.0.0.0:5000", "--workers=4"]
