# ============================================================
# Dockerfile para WORKER
# Cada integrante del equipo corre esto en su PC:
#
#   docker build -t worker .
#   docker run --rm -e COORDINATOR_IP=192.168.1.X -e WORKER_ID=worker-1 worker
#
#   # Worker especializado en video que procesa 2 sub-tareas a la vez:
#   docker run --rm -e COORDINATOR_IP=192.168.1.X -e WORKER_ID=worker-video \
#     -e WORKER_POOLS=video -e WORKER_CONCURRENCY=2 worker
#
# Reemplazar 192.168.1.X con la IP del nodo coordinador
# ============================================================

FROM python:3.11-slim

# Instalar FFmpeg (necesario para procesar multimedia)
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY worker.py .

# Variables de entorno (se sobreescriben con -e al correr)
ENV COORDINATOR_IP=localhost
ENV WORKER_ID=worker-default
ENV WORKER_POOLS=video,audio,ligera
ENV WORKER_CONCURRENCY=1
ENV PYTHONUNBUFFERED=1

CMD ["python", "worker.py"]
