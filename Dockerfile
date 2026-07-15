# Multi-stage build for dagster-codekit proxy
# Stage 1: build dependencies
FROM python:3.11-slim AS builder

WORKDIR /install

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY dagster_codekit/ ./dagster_codekit/

RUN pip install --no-cache-dir --prefix=/install \
    ".[kubernetes,postgres]"

# Stage 2: runtime
FROM python:3.11-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 curl && \
    rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --shell /bin/bash codekit

COPY --from=builder /install /usr/local

RUN mkdir -p /opt/dagster/dagster_home && chown -R codekit:codekit /opt/dagster

USER codekit
WORKDIR /opt/dagster

EXPOSE 8000 4000

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
    CMD curl -f http://localhost:8000/health/live || exit 1

ENTRYPOINT ["dagster-codekit"]
CMD ["start"]
