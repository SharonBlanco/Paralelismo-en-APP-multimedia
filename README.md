# Plataforma Distribuida de Procesamiento Multimedia por Casos
### IC-6600 · Principios de Sistemas Operativos — TEC Campus San Carlos · II Semestre 2026

Plataforma que recibe **casos** (conjuntos de archivos de audio, video e imágenes, homogéneos o heterogéneos), los descompone en sub-tareas según el tipo de cada archivo y las distribuye entre **workers en computadoras distintas** mediante colas de RabbitMQ. Monitorea los recursos de cada nodo y genera un **reporte consolidado** por caso.

| Documento | Contenido |
|---|---|
| [docs/ARQUITECTURA.md](docs/ARQUITECTURA.md) | Arquitectura, flujo, routing, pools especializados (Unidad 1), estados, barrier/join, tolerancia a fallos, API |
| [docs/MANUAL_USUARIO.md](docs/MANUAL_USUARIO.md) | Uso del dashboard y del cliente de línea de comandos |
| [docs/INFORME_PRUEBAS.md](docs/INFORME_PRUEBAS.md) | Plan de pruebas y plantilla del informe |

```
Proyecto/
├── coordinador/app.py        # API + routing + barrier/join + dashboard + reportes
├── worker/worker.py          # Worker (FFmpeg) — Dockerfile incluido
├── cliente/cliente.py        # Cliente: envío, carga concurrente, casos automáticos, métricas
├── scripts/generar_dataset.py# Dataset sintético (~500 archivos con metadatos)
├── docker-compose.yml        # RabbitMQ + PostgreSQL
├── init.sql                  # Esquema de la base de datos
└── docs/
```

---

## Arquitectura en una imagen

```
                 Cliente (cliente.py / navegador)
                              │ HTTP
                              ▼
┌───────────────────── Nodo coordinador ─────────────────────┐
│  Coordinador (FastAPI) ── PostgreSQL     uploads/ results/  │
│        │  ▲                                                 │
│        ▼  │                                                 │
│  RabbitMQ: tareas.video · tareas.audio · tareas.ligera      │
│            results                                          │
└────────────────────────────────────────────────────────────┘
        │ AMQP + HTTP             │                   │
        ▼                         ▼                   ▼
  ┌───────────┐            ┌───────────┐        ┌───────────┐
  │ worker-1  │            │ worker-2  │        │ worker-3  │
  │ PC #1     │            │ PC #2     │        │ PC #3     │
  └───────────┘            └───────────┘        └───────────┘
```

---

## 1. Nodo coordinador

Requisitos: Docker, Python 3.11 o superior, y FFmpeg (solo si también correrá un worker ahí).

```bash
cd Proyecto
docker compose up -d                       # RabbitMQ (5672, panel 15672) + PostgreSQL (5435)
python3 -m venv .venv
source .venv/bin/activate
pip install -r coordinador/requirements.txt -r worker/requirements.txt
python coordinador/app.py                  # correrlo desde la carpeta Proyecto
```

- Dashboard: `http://localhost:8000` · API: `http://localhost:8000/docs`
- Panel de RabbitMQ: `http://localhost:15672` (admin / admin123)
- IP para los workers: `hostname -I` (Linux) o `ipconfig` (Windows)
- Si la red tiene firewall, abrir los puertos **5672** y **8000**.

---

## 2. Nodos worker (uno por computadora)

Copiar la carpeta `worker/` a cada PC. Opciones de configuración:

| Variable | Valor por defecto | Descripción |
|---|---|---|
| `COORDINATOR_IP` | `localhost` | IP del nodo coordinador |
| `WORKER_ID` | `worker-<hostname>` | Nombre único del worker |
| `WORKER_POOLS` | `video,audio,ligera` | Pools que atiende (los tres = genérico) |
| `WORKER_CONCURRENCY` | `1` | Sub-tareas en paralelo en este nodo |
| `CPU_HIGH` / `CPU_LOW` | `90` / `70` | Umbrales de CPU (%) para pausar / reanudar |

**Con Docker** (en primer plano; se detiene con Ctrl+C):

```bash
cd worker
docker build -t worker .
docker run --rm --name worker-1 -e COORDINATOR_IP=192.168.1.100 -e WORKER_ID=worker-1 worker

# Worker especializado en video que procesa 2 a la vez (PC con más núcleos):
docker run --rm --name worker-video -e COORDINATOR_IP=192.168.1.100 \
  -e WORKER_ID=worker-video -e WORKER_POOLS=video -e WORKER_CONCURRENCY=2 worker
```

- Si el worker corre **en la misma máquina** que el coordinador, agregue `--network host` y use `COORDINATOR_IP=localhost`.
- En Linux, si aparece `permission denied ... docker.sock`, use `sudo` o ejecute `sudo usermod -aG docker $USER` y vuelva a iniciar sesión.

**Sin Docker** (requiere FFmpeg instalado):

```bash
pip install -r worker/requirements.txt
cd worker
COORDINATOR_IP=192.168.1.100 WORKER_ID=worker-2 python worker.py
```

> Cada vez que cambie `worker.py` hay que reiniciar el worker y, si usa Docker, reconstruir la imagen. El dashboard marca **⚠ desactualizado** a los workers con una versión vieja.

---

## 3. Dataset y pruebas

```bash
python scripts/generar_dataset.py                # ~480 archivos en 34 casos → ./dataset_prueba

# Un caso
python cliente/cliente.py enviar ./dataset_prueba/caso_022_heterogeneo

# Toda la carga, 4 envíos concurrentes, con métricas para el informe
python cliente/cliente.py carga ./dataset_prueba --concurrentes 4 --metricas pruebas/carga.csv

# Generación automática de casos agrupando por metadatos
python cliente/cliente.py auto ./dataset_prueba --por evento --metricas pruebas/por_evento.csv

python cliente/cliente.py resumen
```

Si el cliente corre en otra PC: `export COORDINATOR_URL=http://<IP>:8000`.

---

## Funcionalidades principales

- **Casos heterogéneos con routing por tipo:** un video genera conversión, extracción de audio y portada. Los audios se convierten o se les extraen metadatos, y las imágenes generan miniatura. Los formatos no soportados se registran como fallidos sin ocupar un worker.
- **Workers genéricos** sobre colas separadas por tipo de carga (`video`, `audio`, `ligera`), con **concurrencia por nodo**. Opcionalmente, un worker se puede especializar con `WORKER_POOLS`.
- **Prioridades reales** (1–10) en RabbitMQ.
- **Barrier/join:** un caso se cierra solo cuando todas sus sub-tareas se resolvieron (`completed`, `partially_completed`, `failed`). Otros estados del caso: `retrying` y `cancelled`.
- **Estados por sub-tarea:** pendiente → asignada → en proceso (con **% de avance**) → completada / fallida / reintentando / cancelada.
- **Tolerancia a fallos:** ACK después del resultado y re-entrega automática si un worker se cae (**redistribución**). Reintentos con espera ante errores transitorios. Idempotencia ante mensajes duplicados.
- **Monitoreo:** CPU, RAM y tareas por worker; detección de desconexión; **pausa automática por saturación**; colas por pool; trazabilidad (mapa de flujo y línea de tiempo del paralelismo).
- **Resultados y reportes:** repositorio central `results/<caso>/`, descarga desde el dashboard o el cliente, y **reporte consolidado** por caso (HTML imprimible y JSON) con resumen agregado, tiempos, workers y metadatos.

## Tecnologías

| Componente | Tecnología |
|---|---|
| Lenguaje | Python 3.11+ |
| Cola de mensajes | RabbitMQ (AMQP, colas con prioridad) |
| Base de datos | PostgreSQL 15 |
| API + dashboard | FastAPI + HTML/JS (SVG) |
| Procesamiento multimedia | FFmpeg / ffprobe |
| Monitoreo de recursos | psutil |
| Contenedores | Docker / Docker Compose |
