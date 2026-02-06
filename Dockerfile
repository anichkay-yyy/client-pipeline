FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml .
RUN pip install --no-cache-dir .

COPY config.yaml .
COPY src/ src/

VOLUME /app/data

CMD ["python", "-m", "pipeline"]
