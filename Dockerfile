FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py ./
RUN mkdir -p /app/output

EXPOSE 8503

CMD ["streamlit", "run", "dashboard.py", "--server.address", "0.0.0.0", "--server.port", "8503", "--server.headless", "true"]
