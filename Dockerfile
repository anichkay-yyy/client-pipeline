FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml .
COPY src/ src/
RUN pip install --no-cache-dir .

COPY config.yaml .

VOLUME /app/data

CMD ["python", "-m", "pipeline"]
