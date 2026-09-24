FROM python:3.12.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt/eruun \
    HOME=/work/home

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 1000 --create-home runner

COPY requirements.txt /opt/eruun/requirements.txt
RUN pip install --no-cache-dir -r /opt/eruun/requirements.txt \
    && mkdir /work && chown 1000:1000 /work
COPY runner.py eruun_environment.py recovery_runtime.py native_checkpoint.py /opt/eruun/
ENV TMPDIR=/work
WORKDIR /work
USER 1000:1000
ENTRYPOINT ["python", "/opt/eruun/runner.py"]
