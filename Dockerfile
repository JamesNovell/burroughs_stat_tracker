FROM python:3.11-slim

# Environment configuration
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=utf-8 \
    DEBIAN_FRONTEND=noninteractive

WORKDIR /app

# Install system dependencies required for pymssql (FreeTDS) and pyodbc
# Must be done as root before switching to non-root user
RUN apt-get update && apt-get install -y \
    wget \
    gnupg2 \
    apt-transport-https \
    ca-certificates \
    unixodbc \
    unixodbc-dev \
    libodbc2 \
    build-essential \
    tzdata \
    libssl-dev \
    libkrb5-dev \
    freetds-dev \
    freetds-bin \
    gcc \
    && update-ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Microsoft SQL Server ODBC driver
RUN wget -qO- https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/11/prod bullseye main" > /etc/apt/sources.list.d/microsoft-prod.list \
    && apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py ./
COPY app/ ./app/
COPY config.json.example ./config.json.example

# Create a non-root user for security
# Do this after installing dependencies but before setting USER
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Default command (can be overridden in docker-compose)
CMD ["python", "main.py"]

