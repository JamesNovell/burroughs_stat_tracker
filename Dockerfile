FROM python:3.11-slim

WORKDIR /app

# Install system dependencies required for pymssql (FreeTDS)
# Must be done as root before switching to non-root user
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    freetds-dev \
    freetds-bin \
    unixodbc-dev \
    gcc \
    && apt-get clean \
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

