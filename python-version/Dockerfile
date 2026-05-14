FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for some python libs
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY indexer.py .

# Create a non-root user
RUN useradd -m mnemosyne
USER mnemosyne

ENTRYPOINT ["python", "indexer.py"]
