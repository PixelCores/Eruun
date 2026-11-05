# Minimal S0 Sandbox image: /bin/sh, git, tar, python3, GNU sleep, UID 1000,
# writable /workspace. The Agent CLIs are copied in at test time, not baked in.
FROM python:3.12.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 1000 --create-home runner \
    && mkdir /workspace && chown 1000:1000 /workspace

USER 1000:1000
WORKDIR /workspace
