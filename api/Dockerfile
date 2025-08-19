FROM python:3.11-slim
# Install system certificates
RUN apt-get update && apt-get install -y ca-certificates

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

WORKDIR /app

RUN apt-get update
RUN apt-get install -y gcc libpq-dev
RUN rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# 👇 Copy the *entire api/ folder* into /app/api
COPY . /app/api

# 👇 Set correct working dir inside container
WORKDIR /app/api

# 👇 Add PYTHONPATH so "api." imports work
ENV PYTHONPATH=/app

EXPOSE 5000

# 👇 run from /app/api/app.py, referencing api.app
CMD ["gunicorn", "api.app:app", "-b", "0.0.0.0:5000", "--workers=4"]
