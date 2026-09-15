FROM python:3.12.12-slim-bookworm

RUN useradd --uid 1000 --create-home agent \
    && mkdir -p /app /workspace /logs/agent /logs/verifier /logs/artifacts /tests /solution /installed-agent \
    && chown -R 1000:1000 /app /workspace /logs /tests /solution /installed-agent

COPY simulate.py /opt/eruun-load/simulate.py

ENV HOME=/home/agent
WORKDIR /app
USER 1000:1000
