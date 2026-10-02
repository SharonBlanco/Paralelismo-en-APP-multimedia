"""
=== WORKER (corre en cada PC del equipo, con o sin Docker) ===
Responsabilidades:
  1. Conectarse a RabbitMQ en el nodo coordinador
  2. Consumir sub-tareas de los pools (colas) que atiende
  3. Procesar archivos con FFmpeg (varias a la vez si WORKER_CONCURRENCY > 1)
  4. Reportar estado (asignada → en proceso → % → resultado) al coordinador
  5. Reportar CPU/RAM periódicamente y dejar de tomar trabajo si se satura

Configuración (variables de entorno):
  COORDINATOR_IP       IP del coordinador (RabbitMQ + API)       [localhost]
  WORKER_ID            nombre único del worker                   [worker-<host>]
  WORKER_POOLS         pools que atiende: video,audio,ligera     [los tres = genérico]
  WORKER_CONCURRENCY   sub-tareas en paralelo en este nodo       [1]
  CPU_HIGH / CPU_LOW   umbrales (%) para pausar / reanudar       [90 / 70]

Uso (sin Docker):
  pip install -r requirements.txt
  COORDINATOR_IP=192.168.1.X WORKER_ID=worker-1 python worker.py

  # Worker especializado en video, 2 tareas a la vez (PC con más núcleos)
  COORDINATOR_IP=192.168.1.X WORKER_ID=worker-video WORKER_POOLS=video WORKER_CONCURRENCY=2 python worker.py

Uso (con Docker):
  docker build -t worker .
  docker run --rm -e COORDINATOR_IP=192.168.1.X -e WORKER_ID=worker-1 worker
"""

import os
import json
import time
import socket
import shutil
import subprocess
import threading
from pathlib import Path
from urllib.parse import quote

import pika
import psutil
import requests


# ============================================================
# CONFIGURACIÓN
# ============================================================
COORDINATOR_IP   = os.getenv("COORDINATOR_IP", "localhost")
COORDINATOR_PORT = os.getenv("COORDINATOR_PORT", "8000")
WORKER_ID        = os.getenv("WORKER_ID", f"worker-{socket.gethostname()}")

RABBIT_HOST = os.getenv("RABBIT_HOST", COORDINATOR_IP)
RABBIT_USER = os.getenv("RABBIT_USER", "admin")
RABBIT_PASS = os.getenv("RABBIT_PASS", "admin123")

COORDINATOR_URL = f"http://{COORDINATOR_IP}:{COORDINATOR_PORT}"

# Pools (colas) especializados por tipo de carga. Ver docs/ARQUITECTURA.md
ALL_POOLS    = ["video", "audio", "ligera"]
WORKER_POOLS = [p.strip() for p in os.getenv("WORKER_POOLS", ",".join(ALL_POOLS)).split(",") if p.strip()]
CONCURRENCY  = max(1, int(os.getenv("WORKER_CONCURRENCY", "1")))
CPU_HIGH     = float(os.getenv("CPU_HIGH", "90"))
CPU_LOW      = float(os.getenv("CPU_LOW", "70"))

# Prefijo opcional de colas (para pruebas aisladas); debe coincidir con el coordinador
QUEUE_PREFIX  = os.getenv("QUEUE_PREFIX", "")
POOL_QUEUES   = {p: f"{QUEUE_PREFIX}tareas.{p}" for p in ALL_POOLS}
RESULTS_QUEUE = f"{QUEUE_PREFIX}results"
QUEUE_ARGS    = {"x-max-priority": 10}   # cola con prioridades (1 = baja, 10 = alta)

# Versión del worker: el coordinador avisa si un worker está desactualizado
WORKER_VERSION = 3

# Carpeta local de trabajo
WORK_DIR = Path("./workspace")
WORK_DIR.mkdir(exist_ok=True)

for p in WORKER_POOLS:
    if p not in ALL_POOLS:
        raise SystemExit(f"Pool desconocido: {p}. Opciones: {', '.join(ALL_POOLS)}")

# Estado compartido entre hilos
state_lock   = threading.Lock()
active_tasks = 0
saturated    = False
consumer     = {"connection": None, "channel": None, "tags": []}


class Cancelled(Exception):
    """El coordinador avisó que el caso fue cancelado"""


# ============================================================
# TRANSFERENCIA DE ARCHIVOS CON EL COORDINADOR (HTTP)
# El worker corre en otra PC/contenedor: no ve el disco del
# coordinador, así que baja el archivo y luego sube el resultado.
# ============================================================
def download_input(subtask: dict) -> Path:
    """Descarga el archivo original desde el coordinador"""
    url = (f"{COORDINATOR_URL}/api/files/"
           f"{quote(subtask['case_id'])}/{quote(subtask['file_name'])}")
    local_dir = WORK_DIR / "input" / subtask["subtask_id"]
    local_dir.mkdir(parents=True, exist_ok=True)
    local_path = local_dir / Path(subtask["file_name"]).name

    with requests.get(url, stream=True, timeout=60) as r:
        if r.status_code == 410:
            raise Cancelled()
        r.raise_for_status()
        with open(local_path, "wb") as fp:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                fp.write(chunk)
    return local_path


def upload_result(subtask_id: str, out_file: str) -> str:
    """Sube el archivo resultante al coordinador; devuelve su ruta allá"""
    with open(out_file, "rb") as fp:
        r = requests.post(
            f"{COORDINATOR_URL}/api/results/{quote(subtask_id)}",
            files={"file": (Path(out_file).name, fp)},
            timeout=300
        )
    r.raise_for_status()
    return r.json()["result_path"]


def report_progress(subtask_id: str, progress: float):
    """Informa el % de avance (best effort: si falla, se ignora)"""
    try:
        requests.post(f"{COORDINATOR_URL}/api/subtasks/{quote(subtask_id)}/progress",
                      json={"progress": round(progress, 1), "worker_id": WORKER_ID}, timeout=3)
    except requests.RequestException:
        pass


# ============================================================
# FFMPEG CON PROGRESO
# ============================================================
def probe_duration(path: str):
    """Duración en segundos (None si no se puede saber, p. ej. imágenes)"""
    try:
        out = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", path],
            capture_output=True, text=True, timeout=30).stdout.strip()
        return float(out) if out and out != "N/A" else None
    except (ValueError, subprocess.SubprocessError):
        return None


def run_ffmpeg(args: list, subtask_id: str = None, duration: float = None):
    """
    Ejecuta FFmpeg. Si se conoce la duración del medio, lee el avance
    (-progress) y lo reporta al coordinador cada ~2 segundos.
    """
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-nostats", "-y"]
    if subtask_id and duration:
        cmd += ["-progress", "pipe:1"]
    cmd += args

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stderr_lines = []
    drain = threading.Thread(target=lambda: stderr_lines.extend(proc.stderr), daemon=True)
    drain.start()

    last_report = 0.0
    for line in proc.stdout:
        if subtask_id and duration and line.startswith(("out_time_us=", "out_time_ms=")):
            try:
                seconds = int(line.split("=", 1)[1]) / 1_000_000
            except ValueError:
                continue
            if time.time() - last_report >= 2:
                report_progress(subtask_id, min(99.0, seconds / duration * 100))
                last_report = time.time()

    proc.wait()
    drain.join(timeout=5)
    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, cmd, stderr="".join(stderr_lines))


# ============================================================
# PROCESAMIENTO DE UNA SUB-TAREA
# ============================================================
def process_subtask(subtask: dict, notify) -> dict:
    """
    Ejecuta la operación multimedia y devuelve el resultado.
    notify(status) informa cambios de estado al coordinador.
    """
    operation  = subtask["operation"]
    file_name  = subtask["file_name"]
    file_type  = subtask.get("file_type")
    target_fmt = subtask.get("target_format") or "mp4"
    subtask_id = subtask["subtask_id"]

    output_path = WORK_DIR / f"{subtask_id}_{Path(file_name).stem}"

    result = {
        "subtask_id": subtask_id,
        "worker_id": WORKER_ID,
        "status": "completed",
        "result_path": "",
        "error_message": "",
        "retryable": False,
    }

    try:
        if operation == "unsupported":
            raise ValueError(f"Formato no soportado: {Path(file_name).suffix or '(sin extensión)'}")

        file_path = str(download_input(subtask))
        notify("processing")
        duration = probe_duration(file_path) if file_type in ("video", "audio") else None

        if operation == "convert_format":
            out_file = f"{output_path}.{target_fmt}"
            run_ffmpeg(["-i", file_path, out_file], subtask_id, duration)
            print(f"    [✓] Convertido: {file_name} → .{target_fmt}")

        elif operation == "extract_audio":
            out_file = f"{output_path}.mp3"
            run_ffmpeg(["-i", file_path, "-vn", "-acodec", "libmp3lame", out_file], subtask_id, duration)
            print(f"    [✓] Audio extraído: {file_name} → .mp3")

        elif operation == "generate_thumbnail":
            if file_type == "video":
                # Portada: fotograma al 10% del video (o al inicio si es muy corto)
                out_file = f"{output_path}_portada.jpg"
                seek = f"{(duration or 0) * 0.1:.2f}"
                run_ffmpeg(["-ss", seek, "-i", file_path, "-vframes", "1", "-vf", "scale=320:-1", out_file])
                print(f"    [✓] Portada: {file_name}")
            else:
                out_file = f"{output_path}_thumb.jpg"
                run_ffmpeg(["-i", file_path, "-vf", "scale=320:-1", out_file])
                print(f"    [✓] Miniatura: {file_name}")

        elif operation == "extract_metadata":
            proc = subprocess.run(
                ["ffprobe", "-v", "quiet", "-print_format", "json",
                 "-show_format", "-show_streams", file_path],
                capture_output=True, text=True, check=True)
            out_file = f"{output_path}_metadata.json"
            with open(out_file, "w") as f:
                f.write(proc.stdout)
            print(f"    [✓] Metadatos: {file_name}")

        else:
            raise ValueError(f"Operación desconocida: {operation}")

        # Enviar el resultado al coordinador
        result["result_path"] = upload_result(subtask_id, out_file)
        Path(out_file).unlink(missing_ok=True)

    except Cancelled:
        result["status"] = "cancelled"
        print(f"    [⊘] Cancelada: {file_name} (el caso fue cancelado)")
    except subprocess.CalledProcessError as e:
        # Error del archivo (corrupto, códec): reintentar no ayuda
        result["status"] = "failed"
        result["error_message"] = f"FFmpeg error: {(e.stderr or '')[:500]}"
        print(f"    [✗] Error FFmpeg: {file_name} — {(e.stderr or '')[:200]}")
    except ValueError as e:
        result["status"] = "failed"
        result["error_message"] = str(e)
        print(f"    [✗] {file_name}: {e}")
    except Exception as e:
        # Red, disco, etc.: error transitorio → el coordinador puede reintentar
        result["status"] = "failed"
        result["retryable"] = True
        result["error_message"] = f"{type(e).__name__}: {str(e)[:450]}"
        print(f"    [✗] Error: {file_name} — {e}")
    finally:
        shutil.rmtree(WORK_DIR / "input" / subtask_id, ignore_errors=True)

    return result


# ============================================================
# CONSUMIDOR DE RABBITMQ
# Cada sub-tarea se procesa en su propio hilo. El hilo principal
# solo atiende la conexión (pika no es thread-safe), y los hilos le
# piden publicar/confirmar con add_callback_threadsafe.
# ============================================================
def subscribe(channel):
    """Empieza a consumir de las colas de los pools de este worker"""
    consumer["tags"] = [
        channel.basic_consume(queue=POOL_QUEUES[p], on_message_callback=on_message)
        for p in WORKER_POOLS
    ]


def set_paused(pause: bool):
    """Pausa/reanuda el consumo (se ejecuta en el hilo de la conexión)"""
    channel = consumer["channel"]
    if not channel or not channel.is_open:
        return
    if pause and consumer["tags"]:
        for tag in consumer["tags"]:
            channel.basic_cancel(tag)
        consumer["tags"] = []
        print(f"  [pausa] CPU saturada: {WORKER_ID} deja de tomar sub-tareas nuevas")
    elif not pause and not consumer["tags"]:
        subscribe(channel)
        print(f"  [reanuda] CPU normal: {WORKER_ID} vuelve a tomar sub-tareas")


def handle_message(connection, channel, delivery_tag, redelivered, body):
    """Hilo de trabajo para una sub-tarea"""
    global active_tasks
    subtask = json.loads(body)

    def send(msg):
        # Publicar desde el hilo de la conexión
        def _publish():
            channel.basic_publish(exchange="", routing_key=RESULTS_QUEUE, body=json.dumps(msg),
                                  properties=pika.BasicProperties(delivery_mode=2))
        connection.add_callback_threadsafe(_publish)

    def notify(status):
        send({"subtask_id": subtask["subtask_id"], "worker_id": WORKER_ID,
              "status": status, "redelivered": redelivered})

    with state_lock:
        active_tasks += 1
    try:
        print(f"\n  [←] Recibida{' (re-entregada)' if redelivered else ''}: "
              f"{subtask['subtask_id']} | {subtask['operation']} | {subtask['file_name']}")
        notify("assigned")
        result = process_subtask(subtask, notify)
    finally:
        with state_lock:
            active_tasks -= 1

    def finish():
        channel.basic_publish(exchange="", routing_key=RESULTS_QUEUE, body=json.dumps(result),
                              properties=pika.BasicProperties(delivery_mode=2))
        # El ACK va después del resultado: si el worker muere antes,
        # RabbitMQ re-entrega la sub-tarea a otro worker.
        channel.basic_ack(delivery_tag=delivery_tag)

    try:
        connection.add_callback_threadsafe(finish)
    except Exception as e:
        print(f"  [!] Conexión perdida; RabbitMQ re-entregará {subtask['subtask_id']}: {e}")


def on_message(ch, method, properties, body):
    threading.Thread(
        target=handle_message,
        args=(consumer["connection"], ch, method.delivery_tag, method.redelivered, body),
        daemon=True
    ).start()


def start_consuming():
    """Bucle principal: consume sub-tareas de los pools asignados"""
    credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(
        host=RABBIT_HOST, credentials=credentials,
        heartbeat=30, blocked_connection_timeout=300
    )

    while True:
        try:
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            for q in POOL_QUEUES.values():
                channel.queue_declare(queue=q, durable=True, arguments=QUEUE_ARGS)
            channel.queue_declare(queue=RESULTS_QUEUE, durable=True)

            # Como máximo CONCURRENCY sub-tareas sin confirmar en este worker
            # (global_qos: el límite es por canal, sumando todos los pools)
            channel.basic_qos(prefetch_count=CONCURRENCY, global_qos=True)

            consumer["connection"], consumer["channel"] = connection, channel
            if saturated:
                consumer["tags"] = []
            else:
                subscribe(channel)

            print(f"[*] {WORKER_ID} esperando sub-tareas de: {', '.join(WORKER_POOLS)} "
                  f"(hasta {CONCURRENCY} a la vez)")
            # No se usa start_consuming(): termina en cuanto no hay consumidores
            # (cuando el worker se pausa por saturación). Este bucle sigue
            # atendiendo la conexión, los heartbeats y los callbacks igual.
            while True:
                connection.process_data_events(time_limit=1)

        except KeyboardInterrupt:
            print(f"\n[*] {WORKER_ID} detenido. Las sub-tareas sin terminar vuelven a la cola.")
            try:
                connection.close()
            except Exception:
                pass
            return
        except pika.exceptions.AMQPConnectionError:
            print(f"[!] No se puede conectar a RabbitMQ ({RABBIT_HOST}). Reintentando en 5s...")
            time.sleep(5)
        except Exception as e:
            print(f"[!] Error: {e}. Reintentando en 5s...")
            time.sleep(5)


# ============================================================
# HEARTBEAT + CONTROL DE SATURACIÓN
# ============================================================
def local_ip():
    """IP de esta máquina en la red que llega al coordinador"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect((COORDINATOR_IP, 80))
            return s.getsockname()[0]
    except OSError:
        return socket.gethostbyname(socket.gethostname())


def heartbeat_loop():
    """
    Cada ~5 s envía métricas al coordinador. Si la CPU supera CPU_HIGH en
    dos mediciones seguidas, el worker deja de tomar sub-tareas nuevas
    (RabbitMQ se las da a otros workers) hasta que baje de CPU_LOW.
    """
    global saturated
    high_readings = 0
    while True:
        cpu = psutil.cpu_percent(interval=1)
        high_readings = high_readings + 1 if cpu >= CPU_HIGH else 0

        if not saturated and high_readings >= 2:
            saturated = True
        elif saturated and cpu <= CPU_LOW:
            saturated = False

        conn = consumer["connection"]
        if conn and conn.is_open:
            try:
                conn.add_callback_threadsafe(lambda p=saturated: set_paused(p))
            except Exception:
                pass

        try:
            requests.post(f"{COORDINATOR_URL}/api/workers/heartbeat", json={
                "worker_id": WORKER_ID,
                "host_address": local_ip(),
                "cpu_usage": cpu,
                "memory_usage": psutil.virtual_memory().percent,
                "active_tasks": active_tasks,
                "version": WORKER_VERSION,
                "pools": WORKER_POOLS,
                "concurrency": CONCURRENCY,
                "saturated": saturated,
            }, timeout=5)
        except requests.RequestException:
            pass  # Si falla, se reintenta en el próximo ciclo
        time.sleep(4)


# ============================================================
# ARRANQUE
# ============================================================
if __name__ == "__main__":
    print("=" * 50)
    print(f"WORKER: {WORKER_ID}  (v{WORKER_VERSION})")
    print(f"Coordinador: {COORDINATOR_URL}")
    print(f"Pools: {', '.join(WORKER_POOLS)} · Concurrencia: {CONCURRENCY}")
    print("=" * 50)

    threading.Thread(target=heartbeat_loop, daemon=True).start()
    start_consuming()
