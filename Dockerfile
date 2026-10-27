# Build the Python package and regenerate Protobuf using a compatible compiler.
FROM python:3.12-slim AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /build
COPY pyproject.toml ./
COPY proto ./proto
COPY generated/__init__.py ./generated/__init__.py
COPY services ./services

# Keep the compiler out of the runtime environment. Both versions are pinned
# because generated Protobuf code cannot require a newer runtime than we install.
RUN python -m venv /opt/codegen \
    && /opt/codegen/bin/pip install "grpcio-tools==1.83.1" "protobuf==7.36.1" \
    && /opt/codegen/bin/python -m grpc_tools.protoc \
       -I proto --python_out=generated proto/telemetry.proto \
    && python -m venv /opt/venv \
    && /opt/venv/bin/pip install "protobuf==7.36.1" . \
    && /opt/venv/bin/pip check

# Import from the installed package, rather than accidentally testing /build.
WORKDIR /tmp
RUN /opt/venv/bin/python -c "from generated.telemetry_pb2 import TelemetryEvent; from services.ingestion_api.main import app"

FROM python:3.12-slim AS runtime

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN useradd --create-home --uid 10001 pulsegrid
WORKDIR /app
COPY --from=builder /opt/venv /opt/venv
COPY --chown=pulsegrid:pulsegrid config ./config
COPY --chown=pulsegrid:pulsegrid scripts ./scripts
USER pulsegrid

EXPOSE 8000
CMD ["uvicorn", "services.ingestion_api.main:app", "--host", "0.0.0.0", "--port", "8000"]
