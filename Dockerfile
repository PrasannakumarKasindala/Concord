# Slim, single-stage image. The core has no third-party deps; [prod] pulls the
# drivers. We install the package so the `concord` entry point is on PATH.
FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml README.md ./
COPY concord ./concord
COPY benchmark ./benchmark

RUN pip install --no-cache-dir ".[prod]"

# Run as non-root. A relay does not need to be root, so it isn't.
RUN useradd --create-home relayuser
USER relayuser

ENTRYPOINT []
CMD ["concord", "relay"]
