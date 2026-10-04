"""
=== COORDINADOR (corre en la tablet) ===
Responsabilidades:
  1. Recibir casos via API REST
  2. Inspeccionar archivos y decidir operación (routing por tipo)
  3. Descomponer caso en sub-tareas
  4. Encolar sub-tareas en RabbitMQ
  5. Escuchar resultados de workers (barrier/join)
  6. Servir dashboard web

Uso:
  pip install -r requirements.txt
  python app.py
  Abrir http://localhost:8000 para el dashboard
  Abrir http://localhost:8000/docs para la API
"""

import os
import json
import shutil
import tempfile
import socket
import subprocess
import uuid
import threading
import time
from datetime import datetime
from html import escape
from zoneinfo import ZoneInfo
from pathlib import Path

import pika
import psycopg2
from psycopg2.extras import RealDictCursor, Json
import urllib.request
import base64
from fastapi import FastAPI, UploadFile, File, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import uvicorn


# ============================================================
# CONFIGURACIÓN — Cambiar la IP de tu tablet acá
# ============================================================
RABBIT_HOST = os.getenv("RABBIT_HOST", "localhost")
RABBIT_USER = os.getenv("RABBIT_USER", "admin")
RABBIT_PASS = os.getenv("RABBIT_PASS", "admin123")

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5435")
PG_DB   = os.getenv("PG_DB", "plataforma_multimedia")
PG_USER = os.getenv("PG_USER", "admin")
PG_PASS = os.getenv("PG_PASS", "admin123")

# Versión de worker esperada (debe coincidir con WORKER_VERSION en worker.py).
# Los workers viejos no la envían y se marcan como desactualizados.
WORKER_VERSION = 5
worker_versions = {}  # worker_id -> versión reportada en el último heartbeat

# Un worker manda heartbeat cada ~6 s; sin noticias en este tiempo → desconectado
OFFLINE_AFTER = 15

# Colas: una por pool de workers (con prioridad) + una de resultados.
# QUEUE_PREFIX permite correr una instancia de prueba aislada (p. ej. "test.").
QUEUE_PREFIX  = os.getenv("QUEUE_PREFIX", "")
POOLS         = ["video", "audio", "ligera"]
POOL_QUEUES   = {p: f"{QUEUE_PREFIX}tareas.{p}" for p in POOLS}
RESULTS_QUEUE = f"{QUEUE_PREFIX}results"
QUEUE_ARGS    = {"x-max-priority": 10}

# Reintentos automáticos ante errores transitorios (red, disco...),
# con espera creciente: 5 s, 10 s, ...
MAX_RETRIES = 2
RETRY_DELAY = 5

# Carpeta donde se guardan archivos subidos y resultados
UPLOAD_DIR  = Path("./uploads")
RESULTS_DIR = Path("./results")
UPLOAD_DIR.mkdir(exist_ok=True)
RESULTS_DIR.mkdir(exist_ok=True)

# Mientras recibe un upload, FastAPI guarda cada archivo en un temporal.
# Por defecto va a /tmp, que en muchas distros es RAM de 1-2 GB: varios
# casos de cientos de MB a la vez lo llenan y el upload falla con 400.
# Se usa una carpeta en disco.
TMP_DIR = (UPLOAD_DIR / ".tmp").resolve()
TMP_DIR.mkdir(exist_ok=True)
tempfile.tempdir = str(TMP_DIR)

# ============================================================
# CONEXIONES
# ============================================================
def get_db():
    """Conexión a PostgreSQL"""
    return psycopg2.connect(
        host=PG_HOST, port=int(PG_PORT), dbname=PG_DB,
        user=PG_USER, password=PG_PASS,
        cursor_factory=RealDictCursor
    )

_DB_TZ = None

def to_local(v: datetime) -> datetime:
    """
    PostgreSQL guarda las horas sin zona (en la zona de la BD, UTC en Docker).
    Se convierten a la hora local del coordinador para mostrarlas bien.
    """
    global _DB_TZ
    if _DB_TZ is None:
        try:
            db = get_db()
            cur = db.cursor()
            cur.execute("SELECT current_setting('TimeZone') AS tz")
            _DB_TZ = ZoneInfo(cur.fetchone()["tz"])
            cur.close()
            db.close()
        except Exception:
            _DB_TZ = ZoneInfo("UTC")
    return v.replace(tzinfo=_DB_TZ).astimezone()


def get_rabbit():
    """Conexión a RabbitMQ"""
    credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(
        host=RABBIT_HOST, credentials=credentials,
        heartbeat=60, blocked_connection_timeout=300
    )
    return pika.BlockingConnection(params)


def declare_queues(channel):
    """Colas de trabajo por pool (con prioridad) + cola de resultados"""
    for q in POOL_QUEUES.values():
        channel.queue_declare(queue=q, durable=True, arguments=QUEUE_ARGS)
    channel.queue_declare(queue=RESULTS_QUEUE, durable=True)


def publish_subtask(channel, st: dict, priority: int):
    """Encola una sub-tarea en la cola del pool que le corresponde"""
    channel.basic_publish(
        exchange="",
        routing_key=POOL_QUEUES[st["pool"]],
        body=json.dumps(st),
        properties=pika.BasicProperties(
            delivery_mode=2,                      # mensaje persistente
            priority=max(1, min(10, int(priority)))
        )
    )


def coordinator_info() -> dict:
    """Nombre e IPs de la máquina del coordinador (para distinguir workers locales)"""
    ips = set()
    try:
        ips.update(subprocess.run(["hostname", "-I"], capture_output=True, text=True,
                                  timeout=5).stdout.split())
    except (OSError, subprocess.SubprocessError):
        pass
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sk:
            sk.connect(("8.8.8.8", 80))                     # no envía nada: solo elige la interfaz
            principal = sk.getsockname()[0]
            ips.add(principal)
    except OSError:
        principal = next(iter(ips), "127.0.0.1")
    return {"hostname": socket.gethostname(), "ip": principal,
            "ips": sorted(i for i in ips if ":" not in i and not i.startswith("172.17."))}


COORDINATOR = coordinator_info()


def is_local(ip: str) -> bool:
    """¿La conexión viene de la misma máquina que el coordinador?"""
    return ip.startswith("127.") or ip == "::1" or ip in COORDINATOR["ips"]


def ensure_long_task_policy():
    """
    RabbitMQ cancela a un consumidor que tarda más de 30 min en confirmar
    un mensaje (consumer_timeout) y re-entrega la tarea: con videos de una
    hora, la sub-tarea rebotaría para siempre. Se define una política que
    sube ese límite a 4 h para las colas de trabajo (API de administración).
    """
    nombre = f"{QUEUE_PREFIX or 'default'}-tareas-largas"
    cuerpo = json.dumps({
        "pattern": "^" + QUEUE_PREFIX.replace(".", "\\.") + "tareas\\.",
        "definition": {"consumer-timeout": 4 * 3600 * 1000},
        "apply-to": "queues",
        "priority": 1,
    }).encode()
    req = urllib.request.Request(
        f"http://{RABBIT_HOST}:{os.getenv('RABBIT_MGMT_PORT', '15672')}/api/policies/%2F/{nombre}",
        data=cuerpo, method="PUT", headers={"Content-Type": "application/json"})
    token = base64.b64encode(f"{RABBIT_USER}:{RABBIT_PASS}".encode()).decode()
    req.add_header("Authorization", f"Basic {token}")
    try:
        urllib.request.urlopen(req, timeout=5)
        print("[*] Política de RabbitMQ: tareas de hasta 4 h sin re-entrega")
    except Exception as e:
        print(f"[!] No se pudo configurar el límite de tiempo de RabbitMQ ({e}). "
              "Las sub-tareas de más de 30 min podrían re-entregarse.")


def ensure_schema():
    """
    Agrega a la BD las columnas nuevas si no existen (para no tener que
    recrear el volumen de PostgreSQL de una instalación anterior).
    """
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        ALTER TABLE cases    ADD COLUMN IF NOT EXISTS metadata JSONB;
        ALTER TABLE subtasks ADD COLUMN IF NOT EXISTS pool VARCHAR(20);
        ALTER TABLE subtasks ADD COLUMN IF NOT EXISTS retries INT DEFAULT 0;
        ALTER TABLE subtasks ADD COLUMN IF NOT EXISTS reassignments INT DEFAULT 0;
        ALTER TABLE subtasks ADD COLUMN IF NOT EXISTS assigned_at TIMESTAMP;
        ALTER TABLE subtasks ADD COLUMN IF NOT EXISTS file_size BIGINT;
        ALTER TABLE subtasks ADD COLUMN IF NOT EXISTS result_info JSONB;
        ALTER TABLE workers  ADD COLUMN IF NOT EXISTS pools VARCHAR(100);
        ALTER TABLE workers  ADD COLUMN IF NOT EXISTS concurrency INT DEFAULT 1;
        ALTER TABLE workers  ADD COLUMN IF NOT EXISTS machine JSONB;
    """)
    db.commit()
    cur.close()
    db.close()


# ============================================================
# ROUTING: decide qué operaciones hacer según el tipo de archivo
# y a qué pool (cola) va cada una.
# ============================================================
VIDEO_EXTENSIONS = {".mp4", ".mkv", ".avi", ".mov"}
AUDIO_EXTENSIONS = {".mp3", ".wav", ".flac", ".ogg"}
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png"}

CONVERT_TO = {
    ".mp4": "mkv", ".mkv": "mp4", ".avi": "mp4", ".mov": "mp4",
    ".wav": "mp3", ".flac": "mp3", ".ogg": "mp3",
}


def file_type_of(file_name: str) -> str:
    ext = Path(file_name).suffix.lower()
    return ("video" if ext in VIDEO_EXTENSIONS else
            "audio" if ext in AUDIO_EXTENSIONS else
            "image" if ext in IMAGE_EXTENSIONS else "unknown")


def determine_subtasks(file_name: str):
    """
    Dado un archivo, devuelve sus sub-tareas y el pool de cada una.
      video → convertir + extraer audio + portada      (pool video: CPU intensivo)
      audio → convertir (wav/flac/ogg) o metadatos (mp3)
      imagen → miniatura                               (pool ligera)
    Un tipo no reconocido genera una sub-tarea 'unsupported' que el
    coordinador marca como fallida sin enviarla a ningún worker.
    """
    ext = Path(file_name).suffix.lower()
    ftype = file_type_of(file_name)

    if ftype == "video":
        return [
            {"operation": "convert_format",     "target_format": CONVERT_TO[ext], "pool": "video"},
            {"operation": "extract_audio",      "target_format": "mp3",           "pool": "video"},
            {"operation": "generate_thumbnail", "target_format": "jpg",           "pool": "video"},
        ]
    if ftype == "audio":
        if ext == ".mp3":
            return [{"operation": "extract_metadata", "target_format": "json", "pool": "ligera"}]
        return [{"operation": "convert_format", "target_format": CONVERT_TO[ext], "pool": "audio"}]
    if ftype == "image":
        return [{"operation": "generate_thumbnail", "target_format": "jpg", "pool": "ligera"}]
    return [{"operation": "unsupported", "target_format": None, "pool": None}]


# ============================================================
# APP FASTAPI
# ============================================================
app = FastAPI(title="Plataforma Multimedia - Coordinador")


@app.post("/api/cases")
async def submit_case(
    case_name: str = Form("caso_sin_nombre"),
    priority: int = Form(5),
    metadata: str = Form(None),
    files: list[UploadFile] = File(...)
):
    """
    Endpoint principal: recibe un caso con archivos.
    1. Guarda los archivos en disco
    2. Routing: para cada archivo determina sus operaciones y pool
    3. Registra el caso y sus sub-tareas en la BD
    4. Encola las sub-tareas en la cola de su pool (con la prioridad del caso)

    `metadata` (opcional) es un JSON con datos del caso y, en "files",
    los metadatos de cada archivo (título, artista, evento, sesión...).
    """
    priority = max(1, min(10, priority))
    try:
        meta = json.loads(metadata) if metadata else None
    except json.JSONDecodeError:
        return JSONResponse({"error": "metadata no es un JSON válido"}, 400)

    case_id = f"case-{uuid.uuid4().hex[:8]}"
    case_dir = UPLOAD_DIR / case_id
    case_dir.mkdir(parents=True, exist_ok=True)

    all_subtasks = []
    for f in files:
        # Solo el nombre, sin carpetas (evita escribir fuera de case_dir)
        f.filename = Path(f.filename).name
        file_path = case_dir / f.filename
        with open(file_path, "wb") as fp:
            shutil.copyfileobj(f.file, fp)

        size = file_path.stat().st_size
        for task in determine_subtasks(f.filename):
            all_subtasks.append({
                "subtask_id": f"st-{uuid.uuid4().hex[:8]}",
                "case_id": case_id,
                "file_name": f.filename,
                "file_path": str(file_path),
                "file_type": file_type_of(f.filename),
                "file_size": size,
                **task,
            })

    queued = [st for st in all_subtasks if st["pool"]]
    unsupported = [st for st in all_subtasks if not st["pool"]]

    db = get_db()
    cur = db.cursor()
    cur.execute("""
        INSERT INTO cases (case_id, case_name, priority, total_subtasks, failed_count, status, metadata)
        VALUES (%s, %s, %s, %s, %s, 'queued', %s)
    """, (case_id, case_name, priority, len(all_subtasks), len(unsupported),
          json.dumps(meta, ensure_ascii=False) if meta else None))

    for st in all_subtasks:
        if st["pool"]:
            cur.execute("""
                INSERT INTO subtasks (subtask_id, case_id, file_name, file_path, file_type,
                                      file_size, operation, target_format, pool, status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'pending')
            """, (st["subtask_id"], case_id, st["file_name"], st["file_path"], st["file_type"],
                  st["file_size"], st["operation"], st["target_format"], st["pool"]))
        else:
            # Formato no soportado: se resuelve aquí mismo, sin gastar un worker
            ext = Path(st["file_name"]).suffix or "(sin extensión)"
            cur.execute("""
                INSERT INTO subtasks (subtask_id, case_id, file_name, file_path, file_type,
                                      file_size, operation, status, error_message, finished_at)
                VALUES (%s,%s,%s,%s,%s,%s,'unsupported','failed',%s,NOW())
            """, (st["subtask_id"], case_id, st["file_name"], st["file_path"], st["file_type"],
                  st["file_size"], f"Formato no soportado: {ext}"))
    db.commit()

    if queued:
        rabbit = get_rabbit()
        channel = rabbit.channel()
        declare_queues(channel)
        for st in queued:
            publish_subtask(channel, st, priority)
            print(f"  [→] Encolada en {st['pool']:6s}: {st['subtask_id']} | {st['operation']} | {st['file_name']}")
        rabbit.close()
        cur.execute("UPDATE cases SET status='processing', started_at=NOW() WHERE case_id=%s", (case_id,))
    else:
        # Nada que procesar (todo no soportado): el caso se cierra ya
        cur.execute("""
            UPDATE cases SET status='failed', started_at=NOW(), finished_at=NOW() WHERE case_id=%s
        """, (case_id,))
    db.commit()
    cur.close()
    db.close()

    if not queued:
        save_report_safe(case_id)

    return {
        "case_id": case_id,
        "case_name": case_name,
        "priority": priority,
        "total_subtasks": len(all_subtasks),
        "unsupported": len(unsupported),
        "status": "processing" if queued else "failed",
        "subtasks": [s["subtask_id"] for s in all_subtasks]
    }


# ============================================================
# BARRIER/JOIN: escucha los mensajes de los workers
# ============================================================
FINAL_STATES = ("completed", "failed", "cancelled")


def refresh_case_status(cur, case_id: str):
    """
    Recalcula el estado agregado del caso. Solo cuando TODAS sus sub-tareas
    terminaron (barrier/join) se decide el estado final. Devuelve el estado
    final si el caso se cerró en esta llamada, o None.
    """
    cur.execute("SELECT status FROM cases WHERE case_id=%s FOR UPDATE", (case_id,))
    case = cur.fetchone()
    if not case or case["status"] in ("completed", "partially_completed", "failed", "cancelled"):
        return None

    cur.execute("""
        SELECT COUNT(*) AS total,
               COUNT(*) FILTER (WHERE status='completed') AS ok,
               COUNT(*) FILTER (WHERE status='failed')    AS bad,
               COUNT(*) FILTER (WHERE status='retrying')  AS retrying
        FROM subtasks WHERE case_id=%s
    """, (case_id,))
    c = cur.fetchone()
    cur.execute("UPDATE cases SET completed_count=%s, failed_count=%s WHERE case_id=%s",
                (c["ok"], c["bad"], case_id))

    if c["ok"] + c["bad"] < c["total"]:
        if case["status"] == "paused":
            return None                      # sigue en pausa hasta que el usuario lo reanude
        new_status = "retrying" if c["retrying"] else "processing"
        cur.execute("UPDATE cases SET status=%s WHERE case_id=%s", (new_status, case_id))
        return None

    # === BARRIER/JOIN: todas las sub-tareas resueltas ===
    final_status = ("completed" if c["bad"] == 0 else
                    "failed" if c["ok"] == 0 else "partially_completed")
    cur.execute("UPDATE cases SET status=%s, finished_at=NOW() WHERE case_id=%s",
                (final_status, case_id))
    return final_status


def republish_subtask(st: dict, priority: int):
    """Vuelve a encolar una sub-tarea en reintento (si el caso no se canceló)"""
    try:
        db = get_db()
        cur = db.cursor()
        cur.execute("SELECT status FROM subtasks WHERE subtask_id=%s", (st["subtask_id"],))
        row = cur.fetchone()
        cur.close()
        db.close()
        if not row or row["status"] != "retrying":
            return
        rabbit = get_rabbit()
        channel = rabbit.channel()
        declare_queues(channel)
        publish_subtask(channel, st, priority)
        rabbit.close()
    except Exception as e:
        print(f"  [!] No se pudo re-encolar {st['subtask_id']}: {e}")


def requeue_paused(cur, case_id: str, priority: int) -> int:
    """
    Vuelve a encolar las sub-tareas en pausa de un caso. Solo las 'paused':
    las que siguen 'pending' todavía tienen su mensaje en RabbitMQ.
    """
    cur.execute("""
        UPDATE subtasks SET status='pending' WHERE case_id=%s AND status='paused'
        RETURNING subtask_id, case_id, file_name, file_path, file_type, operation, target_format, pool
    """, (case_id,))
    filas = cur.fetchall()
    if filas:
        rabbit = get_rabbit()
        channel = rabbit.channel()
        declare_queues(channel)
        for st in filas:
            publish_subtask(channel, dict(st), priority)
        rabbit.close()
    return len(filas)


def handle_worker_message(cur, msg: dict):
    """
    Procesa un mensaje de un worker. Estados posibles:
      assigned   → el worker recibió la sub-tarea (puede ser una re-entrega)
      processing → descargó el archivo y empezó a procesar
      completed / failed / cancelled → resultado final
    Devuelve el case_id si el caso se cerró con este mensaje.
    """
    subtask_id = msg["subtask_id"]
    status = msg["status"]
    worker_id = msg.get("worker_id", "unknown")

    cur.execute("SELECT * FROM subtasks WHERE subtask_id=%s FOR UPDATE", (subtask_id,))
    st = cur.fetchone()
    if not st:
        return None                     # caso eliminado: se ignora
    if st["status"] in FINAL_STATES:
        return None                     # duplicado (p. ej. re-entrega): ya estaba resuelta

    if status == "assigned":
        # Si ya la tenía otro worker, es una redistribución (ese worker cayó)
        if st["assigned_worker"] and st["assigned_worker"] != worker_id and st["status"] in ("assigned", "processing"):
            cur.execute("UPDATE subtasks SET reassignments = reassignments + 1 WHERE subtask_id=%s", (subtask_id,))
            print(f"  [↻] {subtask_id} redistribuida: {st['assigned_worker']} → {worker_id}")
        cur.execute("""
            UPDATE subtasks SET status='assigned', assigned_worker=%s, assigned_at=NOW(),
                                started_at=NULL, progress=0
            WHERE subtask_id=%s
        """, (worker_id, subtask_id))
        refresh_case_status(cur, st["case_id"])
        return None

    if status == "processing":
        cur.execute("""
            UPDATE subtasks SET status='processing', assigned_worker=%s, started_at=NOW()
            WHERE subtask_id=%s
        """, (worker_id, subtask_id))
        return None

    if status == "paused":
        # El worker tomó una sub-tarea de un caso en pausa y la devolvió sin
        # procesarla: queda "en pausa" hasta que el caso se reanude.
        cur.execute("""
            UPDATE subtasks SET status='paused', assigned_worker=NULL, started_at=NULL, progress=0
            WHERE subtask_id=%s
        """, (subtask_id,))
        cur.execute("SELECT status, priority FROM cases WHERE case_id=%s", (st["case_id"],))
        case = cur.fetchone()
        if case and case["status"] != "paused":
            # Se reanudó mientras este aviso viajaba: re-encolar ya
            requeue_paused(cur, st["case_id"], case["priority"])
        return None

    if status == "cancelled":
        cur.execute("UPDATE subtasks SET status='cancelled', finished_at=NOW() WHERE subtask_id=%s",
                    (subtask_id,))
        return None

    # Resultado final: completed / failed
    error_msg = msg.get("error_message", "") or None
    if status == "failed" and msg.get("retryable") and st["retries"] < MAX_RETRIES:
        # Error transitorio: volver a encolar (estado 'retrying')
        cur.execute("""
            UPDATE subtasks SET status='retrying', retries = retries + 1, error_message=%s,
                                assigned_worker=NULL, started_at=NULL, progress=0
            WHERE subtask_id=%s
        """, (error_msg, subtask_id))
        cur.execute("SELECT priority FROM cases WHERE case_id=%s", (st["case_id"],))
        priority = cur.fetchone()["priority"]
        delay = RETRY_DELAY * (st["retries"] + 1)
        threading.Timer(delay, republish_subtask, args=({
            "subtask_id": subtask_id, "case_id": st["case_id"], "file_name": st["file_name"],
            "file_path": st["file_path"], "file_type": st["file_type"], "operation": st["operation"],
            "target_format": st["target_format"], "pool": st["pool"],
        }, priority)).start()
        print(f"  [⟳] Reintento {st['retries'] + 1}/{MAX_RETRIES} de {subtask_id} en {delay} s: {error_msg}")
        refresh_case_status(cur, st["case_id"])
        return None

    cur.execute("""
        UPDATE subtasks
        SET status=%s, error_message=%s, result_path=%s, assigned_worker=%s,
            finished_at=NOW(), progress=CASE WHEN %s='completed' THEN 100 ELSE progress END,
            started_at=COALESCE(started_at, NOW()), result_info=%s
        WHERE subtask_id=%s
    """, (status, error_msg, msg.get("result_path") or None, worker_id, status,
          Json(msg["info"]) if msg.get("info") else None, subtask_id))
    print(f"  [✓] Resultado: {subtask_id} → {status} (worker: {worker_id})")

    final_status = refresh_case_status(cur, st["case_id"])
    if final_status:
        print(f"  [★] Caso {st['case_id']} finalizado → {final_status}")
        return st["case_id"]
    return None


def listen_results():
    """
    Hilo que escucha la cola de resultados. Los workers informan aquí
    cada cambio de estado de sus sub-tareas.
    """
    while True:
        try:
            rabbit = get_rabbit()
            channel = rabbit.channel()
            declare_queues(channel)

            def on_result(ch, method, properties, body):
                msg = json.loads(body)
                db = get_db()
                cur = db.cursor()
                try:
                    closed_case = handle_worker_message(cur, msg)
                    db.commit()
                except Exception as e:
                    db.rollback()
                    closed_case = None
                    print(f"  [!] Error procesando mensaje {msg}: {e}")
                finally:
                    cur.close()
                    db.close()

                # Con el caso cerrado, generar su reporte consolidado
                if closed_case:
                    save_report_safe(closed_case)
                ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(queue=RESULTS_QUEUE, on_message_callback=on_result)
            print("[*] Escuchando resultados de workers...")
            channel.start_consuming()

        except Exception as e:
            print(f"[!] Error en listener de resultados: {e}")
            time.sleep(5)  # Reintentar


# ============================================================
# ENDPOINTS DEL DASHBOARD
# ============================================================
@app.get("/api/cases")
def list_cases():
    """Lista todos los casos con su estado"""
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        SELECT c.*,
               COUNT(s.*) FILTER (WHERE s.status IN ('pending', 'retrying'))    AS n_waiting,
               COUNT(s.*) FILTER (WHERE s.status IN ('assigned', 'processing')) AS n_running,
               COUNT(s.*) FILTER (WHERE s.status = 'completed')                 AS n_completed,
               COUNT(s.*) FILTER (WHERE s.status = 'failed')                    AS n_failed,
               COUNT(s.*) FILTER (WHERE s.status = 'cancelled')                 AS n_cancelled,
               COUNT(s.*) FILTER (WHERE s.status = 'paused')                    AS n_paused
        FROM cases c LEFT JOIN subtasks s ON s.case_id = c.case_id
        GROUP BY c.case_id ORDER BY c.created_at DESC
    """)
    cases = cur.fetchall()
    cur.close()
    db.close()
    # Convertir datetimes a string
    for c in cases:
        for k, v in c.items():
            if isinstance(v, datetime):
                c[k] = to_local(v).isoformat()
    return cases


@app.delete("/api/cases/{case_id}")
def delete_case(case_id: str):
    """
    Elimina un caso: sus sub-tareas, el registro del caso y sus archivos
    (originales y resultados). Si quedaban sub-tareas en la cola, el worker
    que las tome no encontrará el archivo y su resultado se ignora.
    """
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT case_id FROM cases WHERE case_id=%s", (case_id,))
    if not cur.fetchone():
        cur.close()
        db.close()
        return JSONResponse({"error": "Caso no encontrado"}, 404)

    cur.execute("DELETE FROM subtasks WHERE case_id=%s", (case_id,))
    deleted_subtasks = cur.rowcount
    cur.execute("DELETE FROM cases WHERE case_id=%s", (case_id,))
    db.commit()
    cur.close()
    db.close()

    for base in (UPLOAD_DIR, RESULTS_DIR):
        folder = base / case_id
        if _inside(base, folder):
            shutil.rmtree(folder, ignore_errors=True)

    print(f"  [x] Caso {case_id} eliminado ({deleted_subtasks} sub-tareas)")
    return {"deleted": case_id, "subtasks": deleted_subtasks}


@app.get("/api/cases/{case_id}")
def get_case(case_id: str):
    """Detalle de un caso con sus sub-tareas"""
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT * FROM cases WHERE case_id=%s", (case_id,))
    case = cur.fetchone()
    cur.execute(
        "SELECT * FROM subtasks WHERE case_id=%s ORDER BY created_at",
        (case_id,)
    )
    subtasks = cur.fetchall()
    cur.close()
    db.close()

    if not case:
        return JSONResponse({"error": "Caso no encontrado"}, 404)

    for item in [case] + subtasks:
        for k, v in item.items():
            if isinstance(v, datetime):
                item[k] = to_local(v).isoformat()

    return {"case": case, "subtasks": subtasks}


def mark_offline(cur):
    """Marca como 'offline' a los workers sin heartbeat reciente"""
    cur.execute("""
        UPDATE workers SET status='offline', active_tasks=0
        WHERE status != 'offline'
          AND last_heartbeat < LOCALTIMESTAMP - make_interval(secs => %s)
    """, (OFFLINE_AFTER,))


@app.delete("/api/workers/{worker_id}")
def delete_worker(worker_id: str):
    """Quita un worker de la lista (si vuelve a mandar heartbeat, reaparece)"""
    db = get_db()
    cur = db.cursor()
    cur.execute("DELETE FROM resource_logs WHERE worker_id=%s", (worker_id,))
    cur.execute("DELETE FROM workers WHERE worker_id=%s", (worker_id,))
    deleted = cur.rowcount
    db.commit()
    cur.close()
    db.close()
    worker_versions.pop(worker_id, None)
    if not deleted:
        return JSONResponse({"error": "Worker no encontrado"}, 404)
    return {"deleted": worker_id}


@app.get("/api/workers")
def list_workers():
    """Estado actual de todos los workers (conectados primero)"""
    db = get_db()
    cur = db.cursor()
    mark_offline(cur)
    db.commit()
    cur.execute("""
        SELECT w.*, EXTRACT(EPOCH FROM LOCALTIMESTAMP - w.last_heartbeat) AS seconds_since,
               (SELECT COUNT(*) FROM subtasks s
                WHERE s.assigned_worker = w.worker_id
                  AND s.status IN ('completed','failed')) AS processed
        FROM workers w ORDER BY (w.status = 'offline'), w.worker_id
    """)
    workers = cur.fetchall()
    cur.close()
    db.close()
    for w in workers:
        for k, v in w.items():
            if isinstance(v, datetime):
                w[k] = to_local(v).isoformat()
        w["seconds_since"] = float(w["seconds_since"] or 0)
        w["same_machine"] = is_local(w["host_address"] or "")
        w["version"] = worker_versions.get(w["worker_id"])
        # None = todavía no mandó heartbeat desde que arrancó el coordinador
        w["outdated"] = (w["version"] != WORKER_VERSION
                         if w["worker_id"] in worker_versions else None)
    return workers


@app.post("/api/workers/heartbeat")
async def worker_heartbeat(request: Request):
    """Los workers reportan su estado periódicamente"""
    data = await request.json()
    worker_versions[data["worker_id"]] = data.get("version")
    if data.get("version") != WORKER_VERSION:
        print(f"  [!] {data['worker_id']} está DESACTUALIZADO "
              f"(versión {data.get('version')}, se espera {WORKER_VERSION}). Reinicialo / reconstruí su imagen.")
    db = get_db()
    cur = db.cursor()

    # IP desde la que llega la conexión: la ve el coordinador, no la informa
    # el worker. Es la prueba de que el worker está en otra máquina.
    ip_real = request.client.host if request.client else data.get("host_address")
    machine = data.get("machine") or {}
    machine["ip_informada"] = data.get("host_address")

    # saturated: el worker pausó el consumo porque su CPU está al límite
    status = ("saturated" if data.get("saturated") else
              "busy" if data["active_tasks"] > 0 else "idle")
    pools = ",".join(data.get("pools") or POOLS)
    cur.execute("""
        INSERT INTO workers (worker_id, host_address, cpu_usage, memory_usage,
                             active_tasks, last_heartbeat, status, pools, concurrency, machine)
        VALUES (%s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s)
        ON CONFLICT (worker_id) DO UPDATE SET
            host_address = EXCLUDED.host_address,
            cpu_usage = EXCLUDED.cpu_usage,
            memory_usage = EXCLUDED.memory_usage,
            active_tasks = EXCLUDED.active_tasks,
            last_heartbeat = NOW(),
            status = EXCLUDED.status,
            pools = EXCLUDED.pools,
            concurrency = EXCLUDED.concurrency,
            machine = EXCLUDED.machine
    """, (
        data["worker_id"], ip_real,
        data["cpu_usage"], data["memory_usage"],
        data["active_tasks"], status, pools, data.get("concurrency", 1), Json(machine)
    ))

    # Log de recursos
    cur.execute("""
        INSERT INTO resource_logs (worker_id, cpu_usage, memory_usage, active_tasks)
        VALUES (%s, %s, %s, %s)
    """, (
        data["worker_id"], data["cpu_usage"],
        data["memory_usage"], data["active_tasks"]
    ))

    db.commit()
    cur.close()
    db.close()
    return {"ok": True}


# ============================================================
# TRANSFERENCIA DE ARCHIVOS CON LOS WORKERS
# Los workers corren en otras PCs/contenedores y no ven este
# disco: descargan el original y suben el resultado por HTTP.
# ============================================================
def _inside(base: Path, path: Path) -> bool:
    """True si path está dentro de base (evita ../ en la URL)"""
    return base.resolve() in path.resolve().parents


@app.post("/api/subtasks/{subtask_id}/progress")
async def subtask_progress(subtask_id: str, request: Request):
    """El worker informa el % de avance de una sub-tarea en proceso"""
    data = await request.json()
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        UPDATE subtasks SET progress=%s
        WHERE subtask_id=%s AND status IN ('assigned', 'processing')
    """, (max(0, min(100, float(data.get("progress", 0)))), subtask_id))
    db.commit()
    cur.close()
    db.close()
    return {"ok": True}


@app.post("/api/cases/{case_id}/pause")
def pause_case(case_id: str):
    """
    Pausa un caso: las sub-tareas que ya se están procesando terminan, pero
    las que esperan no arrancan. Si un worker toma una de la cola, el
    coordinador le responde 423 y el worker la devuelve sin procesarla.
    """
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT status FROM cases WHERE case_id=%s FOR UPDATE", (case_id,))
    case = cur.fetchone()
    if not case:
        cur.close(); db.close()
        return JSONResponse({"error": "Caso no encontrado"}, 404)
    if case["status"] not in ("queued", "processing", "retrying"):
        cur.close(); db.close()
        return JSONResponse({"error": f"No se puede pausar un caso {case['status']}"}, 409)
    cur.execute("UPDATE cases SET status='paused' WHERE case_id=%s", (case_id,))
    db.commit()
    cur.close()
    db.close()
    print(f"  [||] Caso {case_id} en pausa")
    return {"paused": case_id}


@app.post("/api/cases/{case_id}/resume")
def resume_case(case_id: str):
    """Reanuda un caso en pausa: sus sub-tareas pausadas vuelven a la cola"""
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT status, priority FROM cases WHERE case_id=%s FOR UPDATE", (case_id,))
    case = cur.fetchone()
    if not case:
        cur.close(); db.close()
        return JSONResponse({"error": "Caso no encontrado"}, 404)
    if case["status"] != "paused":
        cur.close(); db.close()
        return JSONResponse({"error": "El caso no está en pausa"}, 409)
    cur.execute("UPDATE cases SET status='processing' WHERE case_id=%s", (case_id,))
    reencoladas = requeue_paused(cur, case_id, case["priority"])
    final = refresh_case_status(cur, case_id)          # por si ya no quedaba nada pendiente
    db.commit()
    cur.close()
    db.close()
    if final:
        save_report_safe(case_id)
    print(f"  [>] Caso {case_id} reanudado ({reencoladas} sub-tareas vuelven a la cola)")
    return {"resumed": case_id, "requeued": reencoladas}


@app.post("/api/cases/{case_id}/cancel")
def cancel_case(case_id: str):
    """
    Cancela un caso: sus sub-tareas pendientes ya no se procesan (el worker
    que las tome recibe 410 al descargar el archivo) y el caso se cierra
    como 'cancelled'. Las que ya estaban procesándose terminan, pero su
    resultado se ignora.
    """
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT status FROM cases WHERE case_id=%s FOR UPDATE", (case_id,))
    case = cur.fetchone()
    if not case:
        cur.close(); db.close()
        return JSONResponse({"error": "Caso no encontrado"}, 404)
    if case["status"] in ("completed", "partially_completed", "failed", "cancelled"):
        cur.close(); db.close()
        return JSONResponse({"error": f"El caso ya terminó ({case['status']})"}, 409)

    cur.execute("""
        UPDATE subtasks SET status='cancelled', finished_at=NOW()
        WHERE case_id=%s AND status NOT IN ('completed', 'failed', 'cancelled')
    """, (case_id,))
    cancelled = cur.rowcount
    cur.execute("UPDATE cases SET status='cancelled', finished_at=NOW() WHERE case_id=%s", (case_id,))
    db.commit()
    cur.close()
    db.close()
    print(f"  [⊘] Caso {case_id} cancelado ({cancelled} sub-tareas sin terminar)")
    save_report_safe(case_id)
    return {"cancelled": case_id, "subtasks": cancelled}


@app.get("/api/files/{case_id}/{file_name}")
def download_input(case_id: str, file_name: str):
    """El worker descarga el archivo original a procesar"""
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT status FROM cases WHERE case_id=%s", (case_id,))
    case = cur.fetchone()
    cur.close()
    db.close()
    if case and case["status"] == "cancelled":
        return JSONResponse({"error": "Caso cancelado"}, 410)
    if case and case["status"] == "paused":
        return JSONResponse({"error": "Caso en pausa"}, 423)
    path = UPLOAD_DIR / case_id / file_name
    if not _inside(UPLOAD_DIR, path) or not path.is_file():
        return JSONResponse({"error": "Archivo no encontrado"}, 404)
    return FileResponse(path, filename=file_name)


@app.post("/api/results/{subtask_id}")
async def upload_result(subtask_id: str, file: UploadFile = File(...)):
    """El worker sube el archivo resultante de una sub-tarea"""
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT case_id FROM subtasks WHERE subtask_id=%s", (subtask_id,))
    row = cur.fetchone()
    cur.close()
    db.close()
    if not row:
        return JSONResponse({"error": "Sub-tarea no encontrada"}, 404)

    dest_dir = RESULTS_DIR / row["case_id"]
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / Path(file.filename).name
    with open(dest, "wb") as fp:
        while chunk := await file.read(1024 * 1024):
            fp.write(chunk)
    return {"result_path": str(dest)}


@app.post("/api/results/{subtask_id}/raw")
async def upload_result_raw(subtask_id: str, request: Request, filename: str):
    """
    El worker sube el resultado como flujo de bytes (sin multipart), para
    no tener que cargar en memoria archivos de cientos de MB.
    """
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT case_id FROM subtasks WHERE subtask_id=%s", (subtask_id,))
    row = cur.fetchone()
    cur.close()
    db.close()
    if not row:
        return JSONResponse({"error": "Sub-tarea no encontrada"}, 404)

    dest_dir = RESULTS_DIR / row["case_id"]
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / Path(filename).name
    with open(dest, "wb") as fp:
        async for chunk in request.stream():
            fp.write(chunk)
    return {"result_path": str(dest)}


@app.get("/api/results/{subtask_id}")
def download_result(subtask_id: str):
    """Descargar el resultado de una sub-tarea desde el dashboard"""
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT result_path FROM subtasks WHERE subtask_id=%s", (subtask_id,))
    row = cur.fetchone()
    cur.close()
    db.close()
    path = Path(row["result_path"]) if row and row["result_path"] else None
    if not path or not _inside(RESULTS_DIR, path) or not path.is_file():
        return JSONResponse({"error": "Resultado no disponible"}, 404)
    return FileResponse(path, filename=path.name)


# ============================================================
# REPORTE CONSOLIDADO POR CASO
# ============================================================
# Cómo se nombra cada (tipo de archivo, operación) en el resumen: (singular, plural)
RESULT_LABELS = {
    ("video", "convert_format"):     ("video convertido", "videos convertidos"),
    ("audio", "convert_format"):     ("audio convertido", "audios convertidos"),
    ("video", "extract_audio"):      ("audio extraído de video", "audios extraídos de video"),
    ("audio", "extract_metadata"):   ("audio con metadatos extraídos", "audios con metadatos extraídos"),
    ("image", "generate_thumbnail"): ("miniatura generada", "miniaturas generadas"),
    ("video", "generate_thumbnail"): ("portada de video generada", "portadas de video generadas"),
}
TYPE_LABELS = {"video": ("video", "videos"), "audio": ("audio", "audios"),
               "image": ("imagen", "imágenes"), "unknown": ("otro", "otros")}
OPERATION_LABELS = {
    "convert_format": "Conversión de formato", "extract_audio": "Extracción de audio",
    "extract_metadata": "Extracción de metadatos", "generate_thumbnail": "Miniatura / portada",
    "unsupported": "No soportado",
}
STATUS_LABELS = {
    "queued": "en cola", "processing": "en proceso", "completed": "completado",
    "partially_completed": "parcialmente completado", "failed": "fallido",
    "pending": "pendiente", "assigned": "asignada", "retrying": "reintentando",
    "cancelled": "cancelado", "paused": "en pausa",
}
# Metadatos de archivo que se muestran en el reporte (si vienen en el caso)
META_FIELDS = ["titulo", "artista", "album", "evento", "sesion", "usuario", "lote", "descripcion"]


def _plural(n, labels):
    return f"{n} {labels[0] if n == 1 else labels[1]}"


def _failure_reason(subtask):
    """Agrupa los errores en motivos legibles para el resumen"""
    msg = subtask["error_message"] or ""
    if subtask["operation"] == "unsupported" or msg.startswith("Formato no soportado"):
        return "formato no soportado"
    if msg.startswith("FFmpeg error"):
        return "error de FFmpeg"
    if "404" in msg or "Not Found" in msg:
        return "archivo no disponible"
    return "otro error"


def _seconds(a, b):
    return round((b - a).total_seconds(), 3) if a and b else None


def build_report(case_id: str):
    """Arma el reporte consolidado de un caso. None si el caso no existe."""
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT * FROM cases WHERE case_id=%s", (case_id,))
    case = cur.fetchone()
    cur.execute("""
        SELECT * FROM subtasks WHERE case_id=%s
        ORDER BY file_type, file_name, created_at
    """, (case_id,))
    subtasks = cur.fetchall()
    cur.close()
    db.close()
    if not case:
        return None

    iso = lambda v: to_local(v).isoformat() if v else None
    case_meta = case["metadata"] or {}
    files_meta = case_meta.get("files", {}) if isinstance(case_meta, dict) else {}
    done = [s for s in subtasks if s["status"] in ("completed", "failed")]
    cancelled = [s for s in subtasks if s["status"] == "cancelled"]
    ok = [s for s in subtasks if s["status"] == "completed"]
    failed = [s for s in subtasks if s["status"] == "failed"]

    # Archivos (agrupados por tipo) con el resultado de cada sub-tarea
    files, by_file = [], {}
    for s in subtasks:
        key = s["file_name"]
        if key not in by_file:
            by_file[key] = {"file_name": key, "file_type": s["file_type"],
                            "metadata": files_meta.get(key), "subtasks": []}
            files.append(by_file[key])
        by_file[key]["subtasks"].append({
            "subtask_id": s["subtask_id"],
            "operation": s["operation"],
            "target_format": s["target_format"],
            "status": s["status"],
            "pool": s.get("pool"),
            "worker": s["assigned_worker"],
            "retries": s.get("retries") or 0,
            "reassignments": s.get("reassignments") or 0,
            "info": s.get("result_info"),
            "started_at": iso(s["started_at"]),
            "finished_at": iso(s["finished_at"]),
            "duration_seconds": _seconds(s["started_at"], s["finished_at"]),
            "error_message": s["error_message"] or None,
            "result_url": f"/api/results/{s['subtask_id']}"
                          if s["status"] == "completed" and s["result_path"] else None,
        })

    files_by_type = {}
    for f in files:
        files_by_type[f["file_type"]] = files_by_type.get(f["file_type"], 0) + 1

    # Agrupado por tipo + operación
    groups = {}
    for s in subtasks:
        k = (s["file_type"], s["operation"], s["target_format"])
        g = groups.setdefault(k, {"file_type": k[0], "operation": k[1], "target_format": k[2],
                                  "total": 0, "completed": 0, "failed": 0, "pending": 0, "cancelled": 0})
        g["total"] += 1
        if s["status"] in ("completed", "failed", "cancelled"):
            g[s["status"]] += 1
        else:
            g["pending"] += 1

    # Por worker
    workers = {}
    for s in subtasks:
        if not s["assigned_worker"]:
            continue
        w = workers.setdefault(s["assigned_worker"], {"subtasks": 0, "completed": 0,
                                                      "failed": 0, "busy_seconds": 0.0})
        w["subtasks"] += 1
        if s["status"] == "completed":
            w["completed"] += 1
        elif s["status"] == "failed":
            w["failed"] += 1
        w["busy_seconds"] += _seconds(s["started_at"], s["finished_at"]) or 0
    for w in workers.values():
        w["busy_seconds"] = round(w["busy_seconds"], 3)

    failures = {}
    for s in failed:
        r = _failure_reason(s)
        failures[r] = failures.get(r, 0) + 1

    # Ventana real de procesamiento (primer inicio → último fin de sub-tarea)
    starts = [s["started_at"] for s in subtasks if s["started_at"]]
    ends = [s["finished_at"] for s in done if s["finished_at"]]
    processing_seconds = _seconds(min(starts), max(ends)) if starts and ends else None
    busy_seconds = sum(_seconds(s["started_at"], s["finished_at"]) or 0 for s in done)

    # Resumen en una línea
    tipos = ", ".join(_plural(n, TYPE_LABELS.get(t, (t, t)))
                      for t, n in sorted(files_by_type.items(), key=lambda x: -x[1]))
    # Sumar por (tipo, operación): mp4→mkv y mov→mp4 cuentan juntos como "videos convertidos"
    exitos = {}
    for g in groups.values():
        if g["completed"] and g["operation"] != "unsupported":
            k = (g["file_type"], g["operation"])
            exitos[k] = exitos.get(k, 0) + g["completed"]
    logros = [_plural(n, RESULT_LABELS.get(k, (f"{k[1]} ({k[0]})",) * 2))
              for k, n in sorted(exitos.items(), key=lambda x: -x[1])]
    summary = f"De {_plural(len(files), ('archivo', 'archivos'))} ({tipos})"
    if logros:
        summary += " — " + ", ".join(logros)
    if failed:
        motivos = ", ".join(f"{n} por {r}" for r, n in sorted(failures.items(), key=lambda x: -x[1]))
        summary += f"; {_plural(len(failed), ('fallido', 'fallidos'))} ({motivos})"
    else:
        summary += "; sin fallos" if done else ""
    if cancelled:
        summary += f"; {_plural(len(cancelled), ('sub-tarea cancelada', 'sub-tareas canceladas'))}"
    pending = len(subtasks) - len(done) - len(cancelled)
    if pending:
        summary += f". Aún {_plural(pending, ('sub-tarea pendiente', 'sub-tareas pendientes'))}"
    summary += "."

    return {
        "case_id": case["case_id"],
        "case_name": case["case_name"],
        "priority": case["priority"],
        "status": case["status"],
        "final": case["status"] in ("completed", "partially_completed", "failed", "cancelled"),
        "metadata": {k: v for k, v in case_meta.items() if k != "files"} if isinstance(case_meta, dict) else None,
        "created_at": iso(case["created_at"]),
        "started_at": iso(case["started_at"]),
        "finished_at": iso(case["finished_at"]),
        "total_seconds": _seconds(case["created_at"], case["finished_at"]),
        "processing_seconds": processing_seconds,
        "sequential_seconds": round(busy_seconds, 3),
        "speedup": round(busy_seconds / processing_seconds, 2) if processing_seconds else None,
        "total_files": len(files),
        "files_by_type": files_by_type,
        "total_subtasks": len(subtasks),
        "completed_subtasks": len(ok),
        "failed_subtasks": len(failed),
        "pending_subtasks": pending,
        "cancelled_subtasks": len(cancelled),
        "retries": sum(s.get("retries") or 0 for s in subtasks),
        "reassignments": sum(s.get("reassignments") or 0 for s in subtasks),
        "failures_by_reason": failures,
        "by_type_and_operation": sorted(groups.values(), key=lambda g: (g["file_type"], g["operation"])),
        "by_worker": workers,
        "files": files,
        "summary": summary,
        "generated_at": datetime.now().astimezone().isoformat(),
    }


def save_report_safe(case_id: str):
    """Genera el reporte sin interrumpir el flujo si algo falla"""
    try:
        path = save_report(case_id)
        print(f"  [R] Reporte consolidado: {path}")
    except Exception as e:
        print(f"  [!] No se pudo generar el reporte de {case_id}: {e}")


def save_report(case_id: str) -> Path:
    """Guarda el reporte en el repositorio de resultados, junto a los archivos del caso"""
    report = build_report(case_id)
    folder = RESULTS_DIR / case_id
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"reporte_{case_id}.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


@app.get("/api/cases/{case_id}/report")
def get_report(case_id: str, download: bool = False):
    """Reporte consolidado del caso en JSON (download=true para bajarlo como archivo)"""
    report = build_report(case_id)
    if not report:
        return JSONResponse({"error": "Caso no encontrado"}, 404)
    headers = {"Content-Disposition": f'attachment; filename="reporte_{case_id}.json"'} if download else None
    return JSONResponse(report, headers=headers)


def _fmt_time(iso_str, with_date=False):
    if not iso_str:
        return "-"
    return datetime.fromisoformat(iso_str).strftime("%d/%m/%Y %H:%M:%S" if with_date else "%H:%M:%S")


def _fmt_dur(sec):
    if sec is None:
        return "-"
    if sec < 60:
        return f"{sec:.1f} s"
    m, r = divmod(round(sec), 60)
    return f"{m} min {r:02d} s"


def _info_text(s: dict, file_type: str) -> str:
    """Resumen legible de lo que informó el worker sobre su resultado"""
    info = s.get("info")
    if not isinstance(info, dict):
        return ""
    op = s["operation"]
    if op == "generate_thumbnail" and "segundo" in info:
        m, sec = divmod(int(info["segundo"]), 60)
        return f"Portada tomada en {m}:{sec:02d} (la mejor de {info.get('candidatos', '?')} candidatas)"
    if op == "extract_metadata":
        if not info.get("fuente"):
            return "Sin coincidencia en servicios externos (solo metadatos técnicos)"
        partes = []
        if info.get("album"):
            partes.append(f"Álbum: {info['album']}" + (f" ({info['fecha'][:4]})" if info.get("fecha") else ""))
        if info.get("genero"):
            partes.append(f"Género: {info['genero']}")
        partes.append(f"Versión: {info.get('version', 'original')}")
        partes.append("Letra: sí" if info.get("letra") else "Letra: no encontrada")
        return " · ".join(partes) + f" · Fuente: {info['fuente']}"
    if op == "convert_format" and "entrada_mb" in info:
        return f"{info['entrada_mb']} MB → {info['salida_mb']} MB"
    return ""


def _meta_html(meta):
    """Metadatos asociados a un archivo, en una línea pequeña bajo su nombre"""
    if not isinstance(meta, dict):
        return ""
    parts = [f"{k}: {meta[k]}" for k in META_FIELDS if meta.get(k)]
    return f'<br><small class="muted">{escape(" · ".join(parts))}</small>' if parts else ""


@app.get("/cases/{case_id}/reporte", response_class=HTMLResponse)
def report_page(case_id: str):
    """Reporte consolidado del caso, listo para ver o imprimir / guardar como PDF"""
    r = build_report(case_id)
    if not r:
        return HTMLResponse("<h1>Caso no encontrado</h1>", 404)
    e = lambda v: escape(str(v)) if v is not None else "-"

    cards = [
        ("Archivos", r["total_files"]),
        ("Sub-tareas exitosas", f'{r["completed_subtasks"]} / {r["total_subtasks"]}'),
        ("Sub-tareas fallidas", r["failed_subtasks"]),
        ("Workers participantes", len(r["by_worker"])),
        ("Tiempo de procesamiento", _fmt_dur(r["processing_seconds"])),
        ("Aceleración (speedup)", f'{r["speedup"]}×' if r["speedup"] else "-"),
        ("Reintentos / redistribuciones", f'{r["retries"]} / {r["reassignments"]}'),
    ]
    if r["cancelled_subtasks"]:
        cards.insert(3, ("Sub-tareas canceladas", r["cancelled_subtasks"]))
    cards_html = "".join(f'<div class="card"><div class="label">{e(l)}</div><div class="value">{e(v)}</div></div>'
                         for l, v in cards)

    groups_html = "".join(
        f'<tr><td>{e(TYPE_LABELS.get(g["file_type"], (g["file_type"],) * 2)[1].capitalize())}</td>'
        f'<td>{e(OPERATION_LABELS.get(g["operation"], g["operation"]))}'
        f'{" → " + e(g["target_format"]) if g["target_format"] else ""}</td>'
        f'<td>{g["total"]}</td><td class="ok">{g["completed"]}</td>'
        f'<td class="{"bad" if g["failed"] else ""}">{g["failed"]}</td><td>{g["pending"] + g["cancelled"]}</td></tr>'
        for g in r["by_type_and_operation"])

    workers_html = "".join(
        f'<tr><td>{e(w)}</td><td>{d["subtasks"]}</td><td class="ok">{d["completed"]}</td>'
        f'<td class="{"bad" if d["failed"] else ""}">{d["failed"]}</td><td>{_fmt_dur(d["busy_seconds"])}</td></tr>'
        for w, d in sorted(r["by_worker"].items())) or '<tr><td colspan="5">Ningún worker tomó sub-tareas todavía</td></tr>'

    failures_html = ""
    if r["failures_by_reason"]:
        failures_html = "<h2>Fallos por motivo</h2><ul>" + "".join(
            f"<li>{n} por {e(m)}</li>" for m, n in r["failures_by_reason"].items()) + "</ul>"

    rows, current_type = [], None
    for f in r["files"]:
        if f["file_type"] != current_type:
            current_type = f["file_type"]
            n = sum(1 for x in r["files"] if x["file_type"] == current_type)
            rows.append(f'<tr class="group"><td colspan="8">'
                        f'{e(_plural(n, TYPE_LABELS.get(current_type, (current_type,) * 2)).capitalize())}</td></tr>')
        for i, s in enumerate(f["subtasks"]):
            if s["status"] == "completed":
                detail = f'<a href="{s["result_url"]}">Descargar</a>' if s["result_url"] else "Listo"
            elif s["status"] == "failed":
                detail = f'<span class="bad">{e((s["error_message"] or "")[:200])}</span>'
            else:
                detail = "-"
            extras = []
            if s["retries"]:
                extras.append(f'{s["retries"]} reintento(s)')
            if s["reassignments"]:
                extras.append(f'redistribuida {s["reassignments"]} vez/veces')
            info_txt = _info_text(s, f["file_type"])
            if info_txt:
                extras.insert(0, info_txt)
            if extras:
                detail += f'<br><small class="muted">{e(" · ".join(extras))}</small>'
            rows.append(
                "<tr>"
                + (f'<td rowspan="{len(f["subtasks"])}" class="file">{e(f["file_name"])}{_meta_html(f.get("metadata"))}</td>'
                   if i == 0 else "")
                + f'<td>{e(OPERATION_LABELS.get(s["operation"], s["operation"]))}'
                  f'{" → " + e(s["target_format"]) if s["target_format"] else ""}</td>'
                + f'<td><span class="status {s["status"]}">{e(STATUS_LABELS.get(s["status"], s["status"]))}</span></td>'
                + f'<td class="nw">{e(s["worker"])}</td><td class="nw">{_fmt_time(s["started_at"])}</td>'
                + f'<td class="nw">{_fmt_time(s["finished_at"])}</td><td class="nw">{_fmt_dur(s["duration_seconds"])}</td>'
                + f"<td>{detail}</td></tr>")

    html = REPORT_TEMPLATE
    for k, v in {
        "__CASE_ID__": e(r["case_id"]), "__CASE_NAME__": e(r["case_name"]),
        "__STATUS__": e(r["status"]), "__STATUS_LABEL__": e(STATUS_LABELS.get(r["status"], r["status"])),
        "__PRIORITY__": e(r["priority"]),
        "__CASE_META__": "".join(f'<span>{e(k.capitalize())}: <b>{e(v)}</b></span>'
                                 for k, v in (r["metadata"] or {}).items() if not isinstance(v, (dict, list))),
        "__CREATED__": _fmt_time(r["created_at"], True), "__STARTED__": _fmt_time(r["started_at"], True),
        "__FINISHED__": _fmt_time(r["finished_at"], True), "__TOTAL__": _fmt_dur(r["total_seconds"]),
        "__SUMMARY__": e(r["summary"]), "__CARDS__": cards_html, "__GROUPS__": groups_html,
        "__WORKERS__": workers_html, "__FAILURES__": failures_html, "__ROWS__": "".join(rows),
        "__GENERATED__": _fmt_time(r["generated_at"], True),
        "__NOTE__": "" if r["final"] else
            '<p class="note">El caso todavía no terminó: este reporte es parcial y se actualiza al recargar.</p>',
    }.items():
        html = html.replace(k, v)
    return html


REPORT_TEMPLATE = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Reporte __CASE_ID__</title>
<style>
    :root { --bg:#ecfeff; --panel:#fff; --border:#a5f3fc; --soft:#cffafe; --primary:#06b6d4;
            --primary-dark:#0e7490; --text:#164e63; --muted:#4b8a99; --critical:#b91c1c; }
    * { margin:0; padding:0; box-sizing:border-box; }
    body { font-family:system-ui,sans-serif; background:var(--bg); color:var(--text); padding:24px 16px; }
    main { max-width:1100px; margin:0 auto; }
    header { display:flex; flex-wrap:wrap; justify-content:space-between; gap:12px; align-items:flex-start; }
    h1 { color:var(--primary-dark); font-size:1.6em; }
    h1 small { display:block; font-size:0.55em; color:var(--muted); font-weight:500; margin-top:2px; }
    h2 { color:var(--primary-dark); font-size:1.05em; margin:24px 0 8px; }
    .actions a, .actions button { margin-left:6px; background:#fff; border:1px solid var(--border); color:var(--primary-dark);
            border-radius:8px; padding:6px 12px; font:inherit; font-size:0.9em; text-decoration:none; cursor:pointer; }
    .meta { display:flex; flex-wrap:wrap; gap:6px 22px; margin:12px 0; font-size:0.9em; color:var(--muted); }
    .meta b { color:var(--text); font-weight:600; }
    .summary { background:#fff; border:1px solid var(--border); border-left:5px solid var(--primary);
               border-radius:10px; padding:14px 16px; font-size:1.05em; line-height:1.5; }
    .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:10px; margin-top:14px; }
    .card { background:#fff; border:1px solid var(--border); border-radius:10px; padding:10px 12px; }
    .card .label { color:var(--muted); font-size:0.78em; }
    .card .value { font-size:1.4em; font-weight:700; }
    .wrap { background:#fff; border:1px solid var(--border); border-radius:10px; overflow-x:auto; }
    table { width:100%; border-collapse:collapse; font-size:0.88em; }
    th, td { padding:7px 10px; text-align:left; border-bottom:1px solid var(--soft); vertical-align:top; }
    th { background:var(--soft); color:var(--primary-dark); font-size:0.8em; text-transform:uppercase; }
    tr.group td { background:#f0fdff; font-weight:700; color:var(--primary-dark); }
    td.file { font-weight:600; min-width:140px; overflow-wrap:anywhere; }
    td.nw { white-space:nowrap; }
    .ok { color:#0f766e; } .bad { color:var(--critical); } .muted { color:var(--muted); font-weight:400; }
    .status { padding:1px 7px; border-radius:4px; font-size:0.9em; white-space:nowrap; }
    .status.completed { background:#ccfbf1; color:#0f766e; } .status.failed { background:#fee2e2; color:#b91c1c; }
    .status.processing, .status.assigned { background:#cffafe; color:#0e7490; } .status.pending { background:#e2e8f0; color:#475569; }
    .status.retrying, .status.partially_completed, .status.paused { background:#fef3c7; color:#b45309; } .status.cancelled { background:#e2e8f0; color:#64748b; }
    .note { margin-top:10px; background:#fef3c7; color:#92400e; border-radius:8px; padding:8px 12px; font-size:0.9em; }
    ul { margin-left:20px; }
    footer { margin-top:20px; color:var(--muted); font-size:0.8em; }
    a { color:var(--primary-dark); }
    @media print { body { background:#fff; padding:0; } .actions { display:none; } .wrap, .card, .summary { break-inside:avoid; } }
</style>
</head>
<body><main>
<header>
    <h1>Reporte consolidado: __CASE_NAME__<small>__CASE_ID__</small></h1>
    <div class="actions">
        <a href="/api/cases/__CASE_ID__/report?download=true">Descargar JSON</a>
        <button onclick="window.print()">Imprimir / PDF</button>
    </div>
</header>
<div class="meta">
    <span>Estado: <b><span class="status __STATUS__">__STATUS_LABEL__</span></b></span>
    <span>Prioridad: <b>__PRIORITY__</b></span>
    <span>Creado: <b>__CREATED__</b></span>
    <span>Inicio: <b>__STARTED__</b></span>
    <span>Fin: <b>__FINISHED__</b></span>
    <span>Duración total: <b>__TOTAL__</b></span>
    __CASE_META__
</div>
__NOTE__
<h2>Resumen agregado</h2>
<div class="summary">__SUMMARY__</div>
<div class="grid">__CARDS__</div>

<h2>Por tipo de archivo y operación</h2>
<div class="wrap"><table>
    <thead><tr><th>Tipo</th><th>Operación</th><th>Total</th><th>Exitosas</th><th>Fallidas</th><th>Pendientes</th></tr></thead>
    <tbody>__GROUPS__</tbody>
</table></div>

<h2>Por worker</h2>
<div class="wrap"><table>
    <thead><tr><th>Worker</th><th>Sub-tareas</th><th>Exitosas</th><th>Fallidas</th><th>Tiempo ocupado</th></tr></thead>
    <tbody>__WORKERS__</tbody>
</table></div>
__FAILURES__
<h2>Detalle por archivo y sub-tarea</h2>
<div class="wrap"><table>
    <thead><tr><th>Archivo</th><th>Operación</th><th>Resultado</th><th>Worker</th><th>Inicio</th><th>Fin</th><th>Duración</th><th>Detalle</th></tr></thead>
    <tbody>__ROWS__</tbody>
</table></div>
<footer>Generado el __GENERATED__ · Plataforma Multimedia Distribuida</footer>
</main></body>
</html>
"""


@app.get("/api/trace")
def get_trace(case_id: str | None = None, last_cases: int = 5):
    """
    Trazabilidad: qué worker procesó cada archivo y cuándo.
    Sin case_id devuelve los últimos `last_cases` casos.
    """
    db = get_db()
    cur = db.cursor()

    if case_id:
        case_ids = [case_id]
    else:
        cur.execute(
            "SELECT case_id FROM cases ORDER BY created_at DESC LIMIT %s",
            (last_cases,)
        )
        case_ids = [r["case_id"] for r in cur.fetchall()]

    cur.execute("""
        SELECT c.case_id, c.case_name, s.subtask_id, s.file_name, s.file_type, s.pool, s.progress,
               s.operation, s.target_format, s.status, s.assigned_worker,
               s.error_message, s.started_at, s.finished_at
        FROM subtasks s JOIN cases c ON c.case_id = s.case_id
        WHERE s.case_id = ANY(%s)
        ORDER BY c.created_at, s.created_at, s.subtask_id
    """, (case_ids,))
    subtasks = cur.fetchall()

    # Solo workers conectados; los desconectados aparecen igual si procesaron algo
    cur.execute("""
        SELECT worker_id, status FROM workers
        WHERE last_heartbeat >= LOCALTIMESTAMP - make_interval(secs => %s)
        ORDER BY worker_id
    """, (OFFLINE_AFTER,))
    workers = cur.fetchall()

    # Hora del servidor de BD, misma base que started_at/finished_at
    cur.execute("SELECT LOCALTIMESTAMP AS now")
    now = cur.fetchone()["now"]

    cur.close()
    db.close()

    for s in subtasks:
        for k, v in s.items():
            if isinstance(v, datetime):
                s[k] = to_local(v).isoformat()

    return {"now": to_local(now).isoformat(), "subtasks": subtasks, "workers": workers}


MB = 1024 ** 2
SIZE_BUCKETS = [               # rangos pensados para ver desde miniaturas hasta archivos de 400–600 MB
    ("< 1 MB", MB),
    ("1 – 10 MB", 10 * MB),
    ("10 – 50 MB", 50 * MB),
    ("50 – 200 MB", 200 * MB),
    ("200 – 400 MB", 400 * MB),
    ("400 – 600 MB", 600 * MB),
    ("> 600 MB", None),
]


@app.get("/api/dataset")
def dataset_stats():
    """
    Variedad de los archivos recibidos (todos los casos): cantidad y volumen
    por tipo, por formato y distribución de tamaños.
    """
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        SELECT case_id, file_name, MIN(file_type) AS file_type,
               MAX(file_size) AS file_size, MIN(file_path) AS file_path
        FROM subtasks GROUP BY case_id, file_name
    """)
    files = cur.fetchall()

    # Archivos de casos anteriores a esta versión: medir el tamaño en disco
    for f in files:
        if f["file_size"] is None and f["file_path"] and Path(f["file_path"]).is_file():
            f["file_size"] = Path(f["file_path"]).stat().st_size
            cur.execute("UPDATE subtasks SET file_size=%s WHERE case_id=%s AND file_name=%s",
                        (f["file_size"], f["case_id"], f["file_name"]))
    db.commit()
    cur.close()
    db.close()

    by_type, by_format = {}, {}
    buckets = [{"label": label, "by_type": {}, "bytes": 0} for label, _ in SIZE_BUCKETS]
    sizes = []
    for f in files:
        t = f["file_type"]
        ext = (Path(f["file_name"]).suffix.lower().lstrip(".") or "sin extensión")
        size = f["file_size"] or 0
        bt = by_type.setdefault(t, {"count": 0, "bytes": 0})
        bt["count"] += 1
        bt["bytes"] += size
        bf = by_format.setdefault(ext, {"format": ext, "file_type": t, "count": 0, "bytes": 0})
        bf["count"] += 1
        bf["bytes"] += size
        if f["file_size"] is not None:
            sizes.append(size)
            for b, (_, limit) in zip(buckets, SIZE_BUCKETS):
                if limit is None or size < limit:
                    b["by_type"][t] = b["by_type"].get(t, 0) + 1
                    b["bytes"] += size
                    break

    sizes.sort()
    return {
        "files": len(files),
        "bytes": sum(sizes),
        "measured": len(sizes),
        "min": sizes[0] if sizes else None,
        "median": sizes[len(sizes) // 2] if sizes else None,
        "max": sizes[-1] if sizes else None,
        "by_type": by_type,
        "by_format": sorted(by_format.values(), key=lambda x: -x["count"]),
        "size_buckets": buckets,
    }


@app.get("/api/stats")
def get_stats():
    """Estadísticas generales para el dashboard"""
    db = get_db()
    cur = db.cursor()

    cur.execute("SELECT COUNT(*) as total, status FROM cases GROUP BY status")
    case_stats = {row["status"]: row["total"] for row in cur.fetchall()}

    cur.execute("SELECT COUNT(*) as total, status FROM subtasks GROUP BY status")
    subtask_stats = {row["status"]: row["total"] for row in cur.fetchall()}

    # Un archivo cuenta como procesado cuando TODAS sus sub-tareas terminaron
    cur.execute("""
        SELECT COUNT(*) AS total,
               COUNT(*) FILTER (WHERE pendientes = 0) AS procesados,
               COUNT(*) FILTER (WHERE pendientes = 0 AND fallidas = 0) AS exitosos
        FROM (
            SELECT case_id, file_name,
                   COUNT(*) FILTER (WHERE status NOT IN ('completed','failed')) AS pendientes,
                   COUNT(*) FILTER (WHERE status = 'failed') AS fallidas
            FROM subtasks WHERE status != 'cancelled'      -- lo cancelado no cuenta
            GROUP BY case_id, file_name
        ) f
    """)
    files = cur.fetchone()

    mark_offline(cur)
    db.commit()
    cur.execute("SELECT COUNT(*) as total FROM workers WHERE status != 'offline'")
    active_workers = cur.fetchone()["total"]

    cur.close()
    db.close()

    return {
        "cases": case_stats,
        "subtasks": subtask_stats,
        "files": files,
        "active_workers": active_workers,
        "queues": queue_stats(),
        "coordinator": COORDINATOR,
    }


_queue_cache = {"at": 0.0, "data": None}

def queue_stats():
    """
    Mensajes esperando y consumidores por cola (pool). Se consulta a
    RabbitMQ como mucho cada 2 s para no abrir una conexión por request.
    """
    if time.time() - _queue_cache["at"] < 2 and _queue_cache["data"] is not None:
        return _queue_cache["data"]
    data = {}
    try:
        rabbit = get_rabbit()
        channel = rabbit.channel()
        declare_queues(channel)
        for pool, q in POOL_QUEUES.items():
            r = channel.queue_declare(queue=q, passive=True).method
            data[pool] = {"queue": q, "waiting": r.message_count, "consumers": r.consumer_count}
        rabbit.close()
    except Exception as e:
        data = {"error": str(e)}
    _queue_cache.update(at=time.time(), data=data)
    return data


# ============================================================
# DASHBOARD HTML (página principal)
# ============================================================
@app.get("/", response_class=HTMLResponse)
def dashboard():
    return """
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <title>Plataforma Multimedia - Dashboard</title>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            :root {
                --bg:#ecfeff; --panel:#ffffff; --border:#a5f3fc;
                --primary:#06b6d4; --primary-dark:#0e7490; --accent:#14b8a6;
                --text:#164e63; --muted:#4b8a99; --soft:#cffafe;
            }
            * { margin:0; padding:0; box-sizing:border-box; }
            body { font-family:system-ui,sans-serif; background:linear-gradient(180deg,#cffafe 0%,var(--bg) 280px); color:var(--text); padding:20px; min-height:100vh; }
            h1 { color:var(--primary-dark); margin-bottom:20px; }
            h2 { color:var(--primary-dark); margin:24px 0 10px; font-size:1.1em; }
            .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(200px,1fr)); gap:12px; margin-bottom:20px; }
            .card { background:var(--panel); border-radius:12px; padding:16px; border:1px solid var(--border); box-shadow:0 2px 6px rgba(6,182,212,0.08); }
            .card .label { color:var(--muted); font-size:0.85em; }
            .card .value { font-size:2em; font-weight:bold; color:var(--primary); }
            .card .value .of { font-size:0.5em; color:var(--muted); font-weight:600; }
            .card .sub { color:var(--muted); font-size:0.78em; margin-top:2px; }
            .table-wrap { background:var(--panel); border:1px solid var(--border); border-radius:12px; overflow-x:auto; }
            table { width:100%; border-collapse:collapse; }
            th,td { padding:8px 12px; text-align:left; border-bottom:1px solid var(--soft); }
            th { background:var(--soft); color:var(--primary-dark); font-size:0.8em; text-transform:uppercase; }
            tbody tr:hover { background:#f0fdfa; }
            .status { padding:2px 8px; border-radius:4px; font-size:0.85em; }
            .status.completed, .status.idle { background:#ccfbf1; color:#0f766e; }
            .status.offline { background:#e2e8f0; color:#64748b; }
            tr.offline-row td { color:#94a3b8; }
            .status.processing, .status.busy { background:#cffafe; color:#0e7490; }
            .status.failed { background:#fee2e2; color:#b91c1c; }
            .status.queued, .status.pending { background:#e2e8f0; color:#475569; }
            .status.partially_completed, .status.retrying, .status.saturated, .status.paused { background:#fef3c7; color:#b45309; }
            .status.assigned { background:#e0f2fe; color:#0369a1; }
            .status.cancelled { background:#e2e8f0; color:#64748b; }
            #cases-table td:first-child, #workers-table td:first-child { white-space:nowrap; }
            .breakdown { display:block; font-size:0.78em; color:var(--muted); margin-top:3px; }
            .pool { display:inline-block; background:var(--soft); color:var(--primary-dark); border-radius:999px; padding:1px 8px; font-size:0.78em; margin:1px 2px 1px 0; }
            .queues { display:grid; grid-template-columns:repeat(auto-fit,minmax(200px,1fr)); gap:12px; margin-bottom:8px; }
            .queue .bar { background:var(--soft); border-radius:4px; height:6px; margin-top:8px; overflow:hidden; }
            .queue .bar div { background:linear-gradient(90deg,var(--accent),var(--primary)); height:100%; }
            .btn-cancel { background:none; border:1px solid #fde68a; color:#b45309; border-radius:6px; padding:3px 10px; font:inherit; font-size:0.85em; cursor:pointer; margin-right:4px; }
            .btn-cancel:hover { background:#fef3c7; }
            .btn-pause { background:none; border:1px solid var(--border); color:var(--primary-dark); border-radius:6px; padding:3px 10px; font:inherit; font-size:0.85em; cursor:pointer; margin-right:4px; }
            .btn-pause:hover { background:var(--soft); }
            .progress-bar { background:var(--soft); border-radius:4px; height:8px; overflow:hidden; display:inline-block; width:100px; vertical-align:middle; }
            .progress-fill { background:linear-gradient(90deg,var(--accent),var(--primary)); height:100%; transition:width 0.3s; }

            /* Formulario de subida */
            .upload { display:grid; grid-template-columns:1fr 140px; gap:12px; }
            .upload label { display:block; font-size:0.85em; color:var(--muted); margin-bottom:4px; }
            .upload input[type=text], .upload input[type=number] { width:100%; padding:8px 10px; border:1px solid var(--border); border-radius:8px; font:inherit; color:var(--text); background:#f8feff; }
            .upload input:focus { outline:2px solid var(--primary); border-color:transparent; }
            .dropzone { grid-column:1 / -1; border:2px dashed var(--primary); border-radius:12px; padding:28px; text-align:center; background:#f0fdff; color:var(--primary-dark); cursor:pointer; transition:background 0.2s; }
            .dropzone.drag { background:var(--soft); }
            .dropzone small { display:block; color:var(--muted); margin-top:6px; }
            .file-list { grid-column:1 / -1; list-style:none; display:flex; flex-wrap:wrap; gap:6px; }
            .file-list li { background:var(--soft); border-radius:999px; padding:4px 10px; font-size:0.85em; display:flex; align-items:center; gap:6px; }
            .file-list button { border:none; background:none; color:var(--primary-dark); cursor:pointer; font-size:1em; }
            .actions { grid-column:1 / -1; display:flex; align-items:center; gap:12px; flex-wrap:wrap; }
            .btn { background:linear-gradient(90deg,var(--accent),var(--primary)); color:#fff; border:none; border-radius:8px; padding:10px 20px; font:inherit; font-weight:600; cursor:pointer; }
            .btn:disabled { opacity:0.5; cursor:not-allowed; }
            a.btn { display:inline-block; text-decoration:none; font-size:0.9em; padding:8px 14px; }
            .btn-light { display:inline-block; text-decoration:none; font-size:0.9em; padding:7px 14px; border:1px solid var(--border); border-radius:8px; color:var(--primary-dark); background:#fff; }
            .case-summary { margin-bottom:12px; }
            #detail-summary { font-size:1em; line-height:1.5; margin-bottom:10px; }
            .report-links { display:flex; gap:8px; flex-wrap:wrap; }
            .btn-del { background:none; border:1px solid #fecaca; color:#b91c1c; border-radius:6px; padding:3px 10px; font:inherit; font-size:0.85em; cursor:pointer; }
            .btn-del:hover { background:#fee2e2; }
            .btn-del:disabled { opacity:0.5; cursor:wait; }
            #upload-msg { font-size:0.9em; }
            #upload-msg.ok { color:#0f766e; }
            #upload-msg.err { color:#b91c1c; }
            @media (max-width:600px) { .upload { grid-template-columns:1fr; } }

            /* Trazabilidad: un color fijo por worker (paleta validada para daltonismo) */
            :root {
                --w1:#2a78d6; --w2:#eb6834; --w3:#1baf7a; --w4:#eda100;
                --w5:#e87ba4; --w6:#008300; --w7:#4a3aa7; --w8:#e34948;
                --w-other:#7c8b93; --queue:#94a3b8; --link:#b6dfe8;
                --critical:#d03b3b; --grid:#e3f4f8;
            }
            /* Variedad de archivos: un color fijo por tipo */
            :root { --t-video:#2a78d6; --t-audio:#eb6834; --t-image:#1baf7a; --t-other:#94a3b8; }
            .variety-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(290px,1fr)); gap:14px; margin-top:14px; }
            .chart { background:#fff; border:1px solid var(--soft); border-radius:10px; padding:12px 14px 14px; }
            .chart h3 { color:var(--primary-dark); font-size:0.92em; margin:0; }
            .chart .hint { color:var(--muted); font-size:0.78em; margin:2px 0 10px; }
            .hbar-row { display:grid; grid-template-columns:96px 1fr auto; align-items:center; gap:8px; font-size:0.8em; margin:6px 0; }
            .hbar-row .name { color:var(--text); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
            .hbar-track { height:12px; }
            .hbar { height:100%; border-radius:0 4px 4px 0; min-width:3px; }
            .hbar-val { color:var(--muted); white-space:nowrap; font-variant-numeric:tabular-nums; }
            .cols { display:flex; align-items:flex-end; gap:6px; height:150px; border-bottom:1px solid #a5d8e3; }
            .col { flex:1; display:flex; flex-direction:column; justify-content:flex-end; height:100%; }
            .col .total { text-align:center; font-size:0.76em; color:var(--text); margin-bottom:3px; font-variant-numeric:tabular-nums; }
            .col .stack { display:flex; flex-direction:column-reverse; gap:2px; }
            .col .stack .seg:last-child { border-radius:4px 4px 0 0; }
            .col-labels { display:flex; gap:6px; margin-top:5px; }
            .col-labels div { flex:1; text-align:center; font-size:0.66em; color:var(--muted); line-height:1.25; }
            .variety details { margin-top:12px; font-size:0.85em; }
            .variety summary { cursor:pointer; color:var(--primary-dark); }
            .variety details table { margin-top:8px; }
            .trace h3 { color:var(--primary-dark); font-size:0.95em; margin:18px 0 2px; }
            .trace .hint { color:var(--muted); font-size:0.82em; margin-bottom:8px; }
            .trace-head { display:flex; flex-wrap:wrap; gap:12px 24px; align-items:center; justify-content:space-between; }
            .trace-head label { font-size:0.85em; color:var(--muted); }
            .trace-head select { margin-left:6px; padding:6px 8px; border:1px solid var(--border); border-radius:8px; font:inherit; color:var(--text); background:#f8feff; }
            .legend { display:flex; flex-wrap:wrap; gap:6px 14px; font-size:0.85em; }
            .legend span { display:inline-flex; align-items:center; gap:6px; }
            .legend i { width:12px; height:12px; border-radius:3px; display:inline-block; }
            .kpis { display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:10px; margin-top:14px; }
            .kpi { background:#f0fdff; border:1px solid var(--soft); border-radius:10px; padding:10px 12px; }
            .kpi .label { color:var(--muted); font-size:0.78em; }
            .kpi .value { font-size:1.4em; font-weight:700; color:var(--text); }
            .svg-wrap { overflow-x:auto; border:1px solid var(--soft); border-radius:10px; background:#fff; }
            .svg-wrap svg { display:block; font-family:system-ui,sans-serif; }
            .empty { padding:24px; color:var(--muted); font-size:0.9em; text-align:center; }
            .node rect { transition:opacity 0.15s; }
            .dim { opacity:0.15; }
            .live { stroke-dasharray:6 4; animation:dash 0.8s linear infinite; }
            .pulse { animation:pulse 1.2s ease-in-out infinite; }
            @keyframes dash { to { stroke-dashoffset:-10; } }
            @keyframes pulse { 50% { opacity:0.55; } }
            @media (prefers-reduced-motion:reduce) { .live, .pulse { animation:none; } }
            .tooltip { position:fixed; pointer-events:none; background:#0e3a47; color:#fff; font-size:0.8em; line-height:1.45; padding:8px 10px; border-radius:8px; max-width:320px; box-shadow:0 4px 14px rgba(0,0,0,0.2); display:none; z-index:10; }
            .tooltip b { color:#a5f3fc; }
            .tooltip i { display:inline-block; width:9px; height:9px; border-radius:2px; margin-right:5px; }
        </style>
    </head>
    <body>
        <h1>Plataforma Multimedia — Dashboard</h1>

        <div class="grid" id="stats"></div>

        <h2>Colas por pool <small style="color:var(--muted);font-weight:400">— sub-tareas esperando en RabbitMQ</small></h2>
        <div class="queues" id="queues"></div>

        <h2>Nuevo caso</h2>
        <form class="card upload" id="upload-form">
            <div>
                <label for="case_name">Nombre del caso</label>
                <input type="text" id="case_name" placeholder="caso_sin_nombre">
            </div>
            <div>
                <label for="priority">Prioridad (1 baja · 10 alta)</label>
                <input type="number" id="priority" min="1" max="10" value="5">
            </div>
            <div class="dropzone" id="dropzone">
                Arrastrá archivos aquí o hacé clic para seleccionarlos
                <small>Video: mp4, mkv, avi, mov · Audio: mp3, wav, flac, ogg · Imagen: jpg, jpeg, png</small>
                <input type="file" id="file-input" multiple hidden
                       accept=".mp4,.mkv,.avi,.mov,.mp3,.wav,.flac,.ogg,.jpg,.jpeg,.png">
            </div>
            <ul class="file-list" id="file-list"></ul>
            <div class="actions">
                <button type="submit" class="btn" id="submit-btn" disabled>Enviar caso</button>
                <span id="upload-msg"></span>
            </div>
        </form>

        <h2>Workers activos</h2>
        <p class="hint" id="nodes-summary" style="margin:-4px 0 8px"></p>
        <div class="table-wrap">
        <table id="workers-table">
            <thead><tr><th>ID</th><th>IP (vista por el coordinador)</th><th>Máquina</th><th>Estado</th><th>Pools</th><th>CPU</th><th>Memoria</th><th>Tareas</th><th>Procesadas</th><th>Versión</th><th></th></tr></thead>
            <tbody></tbody>
        </table>
        </div>

        <h2>Casos de procesamiento</h2>
        <div class="table-wrap">
        <table id="cases-table">
            <thead><tr><th>ID</th><th>Nombre</th><th>Estado</th><th>Progreso</th><th>Prioridad</th><th>Creado</th><th></th></tr></thead>
            <tbody></tbody>
        </table>
        </div>

        <h2>Variedad de archivos recibidos</h2>
        <div class="card variety">
            <div class="trace-head">
                <p class="hint" style="margin:0">Tipos, formatos y tamaños de todos los archivos enviados al sistema.</p>
                <div class="legend" id="variety-legend"></div>
            </div>
            <div class="kpis" id="variety-kpis"></div>
            <div class="variety-grid">
                <div class="chart">
                    <h3>Archivos por tipo</h3>
                    <p class="hint">Cantidad y volumen total</p>
                    <div id="chart-types"></div>
                </div>
                <div class="chart">
                    <h3>Formatos</h3>
                    <p class="hint">Cantidad de archivos por extensión</p>
                    <div id="chart-formats"></div>
                </div>
                <div class="chart">
                    <h3>Distribución de tamaños</h3>
                    <p class="hint">Archivos por rango de tamaño, según tipo</p>
                    <div id="chart-sizes"></div>
                </div>
            </div>
            <details>
                <summary>Ver datos en tabla</summary>
                <div class="table-wrap"><table id="variety-table">
                    <thead><tr><th>Formato</th><th>Tipo</th><th>Archivos</th><th>Volumen</th></tr></thead>
                    <tbody></tbody>
                </table></div>
            </details>
        </div>

        <h2>Trazabilidad: qué worker procesó cada archivo</h2>
        <div class="card trace">
            <div class="trace-head">
                <label>Caso
                    <select id="trace-case"><option value="">Últimos 5 casos</option></select>
                </label>
                <div class="legend" id="trace-legend"></div>
            </div>
            <div class="kpis" id="trace-kpis"></div>

            <h3>Mapa de flujo</h3>
            <p class="hint">Caso → archivo → sub-tarea → worker. Pasá el mouse por cualquier nodo para resaltar su recorrido.</p>
            <div class="svg-wrap" id="flow-wrap"><svg id="flow-map"></svg></div>

            <h3>Línea de tiempo por worker</h3>
            <p class="hint">Cada fila es un worker. Las barras que se superponen en vertical son sub-tareas ejecutándose al mismo tiempo (paralelismo). Mové el mouse para ver qué corría en cada instante.</p>
            <div class="svg-wrap" id="timeline-wrap"><svg id="timeline"></svg></div>
        </div>
        <div class="tooltip" id="tooltip"></div>

        <div id="case-detail" style="display:none">
            <h2>Sub-tareas del caso <span id="detail-case-id"></span></h2>
            <div class="card case-summary">
                <div id="detail-summary"></div>
                <div class="report-links">
                    <a id="report-link" class="btn" target="_blank">Ver reporte consolidado</a>
                    <a id="report-json" class="btn-light">Descargar JSON</a>
                </div>
            </div>
            <div class="table-wrap">
            <table id="subtasks-table">
                <thead><tr><th>ID</th><th>Archivo</th><th>Operación</th><th>Pool</th><th>Estado</th><th>Worker</th><th>Duración</th><th>Resultado</th></tr></thead>
                <tbody></tbody>
            </table>
            </div>
        </div>

        <script>
            // ===== Subida de archivos =====
            let selectedFiles = [];
            const dropzone = document.getElementById('dropzone');
            const fileInput = document.getElementById('file-input');
            const fileList = document.getElementById('file-list');
            const submitBtn = document.getElementById('submit-btn');
            const uploadMsg = document.getElementById('upload-msg');

            function renderFiles() {
                fileList.innerHTML = '';
                selectedFiles.forEach((f, i) => {
                    const li = document.createElement('li');
                    li.textContent = `${f.name} (${(f.size/1024/1024).toFixed(2)} MB)`;
                    const rm = document.createElement('button');
                    rm.type = 'button';
                    rm.textContent = '×';
                    rm.title = 'Quitar';
                    rm.onclick = () => { selectedFiles.splice(i, 1); renderFiles(); };
                    li.appendChild(rm);
                    fileList.appendChild(li);
                });
                submitBtn.disabled = selectedFiles.length === 0;
            }

            function addFiles(files) {
                for (const f of files) selectedFiles.push(f);
                renderFiles();
            }

            dropzone.addEventListener('click', () => fileInput.click());
            fileInput.addEventListener('change', () => { addFiles(fileInput.files); fileInput.value = ''; });
            dropzone.addEventListener('dragover', e => { e.preventDefault(); dropzone.classList.add('drag'); });
            dropzone.addEventListener('dragleave', () => dropzone.classList.remove('drag'));
            dropzone.addEventListener('drop', e => {
                e.preventDefault();
                dropzone.classList.remove('drag');
                addFiles(e.dataTransfer.files);
            });

            document.getElementById('upload-form').addEventListener('submit', e => {
                e.preventDefault();
                const form = new FormData();
                form.append('case_name', document.getElementById('case_name').value || 'caso_sin_nombre');
                form.append('priority', document.getElementById('priority').value || '5');
                selectedFiles.forEach(f => form.append('files', f));

                submitBtn.disabled = true;
                uploadMsg.className = '';
                uploadMsg.textContent = 'Subiendo... 0%';

                // XHR para poder mostrar el progreso de la subida
                const xhr = new XMLHttpRequest();
                xhr.open('POST', '/api/cases');
                xhr.upload.onprogress = ev => {
                    if (ev.lengthComputable)
                        uploadMsg.textContent = `Subiendo... ${Math.round(ev.loaded/ev.total*100)}%`;
                };
                xhr.onload = () => {
                    if (xhr.status >= 200 && xhr.status < 300) {
                        const r = JSON.parse(xhr.responseText);
                        uploadMsg.className = 'ok';
                        uploadMsg.textContent = `Caso ${r.case_id} creado con ${r.total_subtasks} sub-tareas`;
                        selectedFiles = [];
                        renderFiles();
                        refresh();
                        showCase(r.case_id);
                    } else {
                        uploadMsg.className = 'err';
                        uploadMsg.textContent = `Error ${xhr.status}: ${xhr.responseText}`;
                        submitBtn.disabled = false;
                    }
                };
                xhr.onerror = () => {
                    uploadMsg.className = 'err';
                    uploadMsg.textContent = 'Error de red al subir los archivos';
                    submitBtn.disabled = false;
                };
                xhr.send(form);
            });

            // ===== Dashboard =====
            async function refresh() {
                // Stats
                const stats = await (await fetch('/api/stats')).json();
                document.getElementById('stats').innerHTML = `
                    <div class="card"><div class="label">Casos totales</div>
                        <div class="value">${Object.values(stats.cases).reduce((a,b)=>a+b,0)}</div></div>
                    <div class="card"><div class="label">En proceso</div>
                        <div class="value">${stats.cases.processing||0}</div></div>
                    <div class="card"><div class="label">Completados</div>
                        <div class="value">${stats.cases.completed||0}</div></div>
                    <div class="card"><div class="label">Workers activos</div>
                        <div class="value">${stats.active_workers}</div></div>
                    <div class="card"><div class="label">Archivos procesados</div>
                        <div class="value">${stats.files.procesados}<span class="of"> / ${stats.files.total}</span></div>
                        <div class="sub">${stats.files.exitosos} sin errores · ${stats.files.procesados - stats.files.exitosos} con fallas
                            · ${(stats.subtasks.completed||0) + (stats.subtasks.failed||0)} sub-tareas</div></div>
                `;

                // Colas por pool
                const q = stats.queues || {};
                document.getElementById('queues').innerHTML = q.error
                    ? `<div class="card">No se pudo consultar RabbitMQ: ${esc(q.error)}</div>`
                    : Object.entries(q).map(([pool, d]) => `
                    <div class="card queue">
                        <div class="label">Pool <b>${pool}</b> · ${POOL_DESC[pool] || ''}</div>
                        <div class="value">${d.waiting}<span class="of"> en espera</span></div>
                        <div class="sub">${d.consumers} worker(s) atendiendo${d.waiting && !d.consumers ? ' · <span style="color:var(--critical)">nadie atiende este pool</span>' : ''}</div>
                        <div class="bar"><div style="width:${Math.min(100, d.waiting * 2)}%"></div></div>
                    </div>`).join('');

                // Workers
                const workers = await (await fetch('/api/workers')).json();
                nodesSummary(workers, stats.coordinator);
                document.querySelector('#workers-table tbody').innerHTML = workers.map(w => {
                    const off = w.status === 'offline';
                    return `
                    <tr class="${off ? 'offline-row' : ''}">
                        <td>${w.worker_id}</td>
                        <td>${esc(w.host_address)}${w.same_machine ? '<br><small style="color:var(--muted)">misma máquina que el coordinador</small>' : ''}</td>
                        <td>${machineCell(w.machine)}</td>
                        <td><span class="status ${w.status}" title="${w.status === 'saturated' ? 'CPU al límite: no toma sub-tareas nuevas hasta que baje' : ''}">${WORKER_STATUS[w.status] || w.status}</span>
                            ${off ? `<small>hace ${fmtAgo(w.seconds_since)}</small>` : ''}</td>
                        <td>${(w.pools || 'video,audio,ligera').split(',').map(p => `<span class="pool">${p}</span>`).join('')}
                            ${w.concurrency > 1 ? `<small title="Sub-tareas en paralelo">×${w.concurrency}</small>` : ''}</td>
                        <td>${off ? '-' : w.cpu_usage + '%'}</td>
                        <td>${off ? '-' : w.memory_usage + '%'}</td>
                        <td>${off ? '-' : w.active_tasks}</td>
                        <td>${w.processed}</td>
                        <td>${off || w.outdated == null ? '-' : w.outdated
                            ? `<span class="status failed" title="Este worker corre código viejo: reinicialo o reconstruí su imagen Docker">desactualizado</span>`
                            : `<span class="status completed">v${w.version}</span>`}</td>
                        <td>${off ? `<button class="btn-del" title="Quitar de la lista" onclick="deleteWorker(this, '${esc(w.worker_id)}')">Quitar</button>` : ''}</td>
                    </tr>`;
                }).join('');

                // Cases
                const cases = await (await fetch('/api/cases')).json();
                casesById = Object.fromEntries(cases.map(c => [c.case_id, c]));
                document.querySelector('#cases-table tbody').innerHTML = cases.map(c => {
                    const total = c.total_subtasks || 1;
                    const done = (c.completed_count||0) + (c.failed_count||0);
                    const pct = Math.round(done/total*100);
                    return `<tr onclick="showCase('${c.case_id}')" style="cursor:pointer">
                        <td>${c.case_id}</td>
                        <td>${c.case_name}</td>
                        <td><span class="status ${c.status}">${CASE_STATUS[c.status] || c.status}</span></td>
                        <td><div class="progress-bar"><div class="progress-fill" style="width:${pct}%"></div></div> ${pct}%
                            <span class="breakdown" >
                                ${caseBreakdown(c)}</span></td>
                        <td>${c.priority}</td>
                        <td>${c.created_at ? new Date(c.created_at).toLocaleString() : ''}</td>
                        <td style="white-space:nowrap">${['queued', 'processing', 'retrying'].includes(c.status)
                            ? `<button class="btn-pause" title="Pausar: lo que está corriendo termina, lo demás espera" onclick="event.stopPropagation(); pauseCase(this, '${c.case_id}', 'pause')">Pausar</button>` : ''}${
                            c.status === 'paused'
                            ? `<button class="btn-pause" title="Reanudar el caso" onclick="event.stopPropagation(); pauseCase(this, '${c.case_id}', 'resume')">Reanudar</button>` : ''}${
                            ['queued', 'processing', 'retrying', 'paused'].includes(c.status)
                            ? `<button class="btn-cancel" title="Cancelar caso" onclick="event.stopPropagation(); cancelCase(this, '${c.case_id}')">Cancelar</button>` : ''}<button class="btn-del" title="Eliminar caso"
                            onclick="event.stopPropagation(); deleteCase(this, '${c.case_id}')">Eliminar</button></td>
                    </tr>`;
                }).join('');

                // Opciones del selector de trazabilidad
                const sel = document.getElementById('trace-case');
                const current = sel.value;
                const opts = ['<option value="">Últimos 5 casos</option>'].concat(
                    cases.map(c => `<option value="${c.case_id}">${c.case_name} (${c.case_id})</option>`));
                const html = opts.join('');
                if (sel.dataset.html !== html) {
                    sel.innerHTML = html;
                    sel.dataset.html = html;
                    sel.value = current;
                }

                if (detailCaseId && casesById[detailCaseId]) renderCaseDetail(detailCaseId);
                refreshVariety();
                await refreshTrace();
            }

            let casesById = {};

            function infoText(s) {
                const i = s.result_info;
                if (!i) return '';
                if (s.operation === 'generate_thumbnail' && i.segundo != null) {
                    const m = Math.floor(i.segundo / 60), sec = String(Math.floor(i.segundo % 60)).padStart(2, '0');
                    return `Portada en ${m}:${sec} (mejor de ${i.candidatos})`;
                }
                if (s.operation === 'extract_metadata') {
                    if (!i.fuente) return 'Sin coincidencia externa';
                    return [i.album ? `${i.album}${i.fecha ? ' (' + i.fecha.slice(0, 4) + ')' : ''}` : null,
                            i.genero, i.letra ? 'con letra' : null, i.fuente].filter(Boolean).join(' · ');
                }
                if (s.operation === 'convert_format' && i.entrada_mb != null) return `${i.entrada_mb} MB → ${i.salida_mb} MB`;
                return '';
            }

            function machineCell(m) {
                if (!m || !m.cpu) return '<small style="color:var(--muted)">sin datos (worker viejo)</small>';
                const nombre = m.docker && /^[0-9a-f]{12}$/.test(m.hostname) ? `contenedor ${m.hostname.slice(0, 6)}` : m.hostname;
                return `<b title="${attr(nombre)}" style="white-space:nowrap">${esc(trunc(nombre, 24))}</b><br><small style="color:var(--muted)">${esc(m.cpu)}<br>${m.nucleos} núcleos · ${m.ram_gb} GB · ${esc(m.so)}</small>`;
            }

            function nodesSummary(workers, coord) {
                const activos = workers.filter(w => w.status !== 'offline');
                const maquinas = new Set(activos.map(w => w.same_machine ? 'coordinador' : w.host_address));
                const remotos = activos.filter(w => !w.same_machine).length;
                document.getElementById('nodes-summary').innerHTML =
                    `<b>${plural(activos.length, 'worker conectado', 'workers conectados')} en ${plural(maquinas.size, 'máquina distinta', 'máquinas distintas')}</b>` +
                    ` (${remotos} en otras computadoras)` +
                    (coord ? ` · Coordinador: ${esc(coord.hostname)} (${esc(coord.ip)})` : '');
            }

            function caseBreakdown(c) {
                return [[c.n_waiting, 'en espera', 'en espera'], [c.n_running, 'en ejecución', 'en ejecución'],
                        [c.n_completed, 'completada', 'completadas'], [c.n_failed, 'fallida', 'fallidas'],
                        [c.n_paused, 'en pausa', 'en pausa'], [c.n_cancelled, 'cancelada', 'canceladas']]
                    .filter(([n]) => n > 0).map(([n, s, p]) => `${n} ${n === 1 ? s : p}`).join(' · ');
            }

            const WORKER_STATUS = { idle: 'libre', busy: 'ocupado', offline: 'desconectado', saturated: 'saturado' };
            const CASE_STATUS = {
                queued: 'en cola', processing: 'en proceso', retrying: 'reintentando', paused: 'en pausa', completed: 'completado',
                partially_completed: 'parcialmente completado', failed: 'fallido', cancelled: 'cancelado'
            };
            const POOL_DESC = { video: 'transcodificación (CPU intensivo)', audio: 'conversión de audio', ligera: 'miniaturas y metadatos' };
            function fmtAgo(sec) {
                if (sec < 60) return Math.round(sec) + ' s';
                if (sec < 3600) return Math.round(sec / 60) + ' min';
                if (sec < 86400) return Math.round(sec / 3600) + ' h';
                return Math.round(sec / 86400) + ' d';
            }

            async function deleteWorker(btn, workerId) {
                if (!confirm(`¿Quitar ${workerId} de la lista? Si se vuelve a conectar, aparece de nuevo.`)) return;
                btn.disabled = true;
                const r = await fetch(`/api/workers/${encodeURIComponent(workerId)}`, { method: 'DELETE' });
                if (!r.ok) { alert('No se pudo quitar: ' + (await r.text())); btn.disabled = false; return; }
                refresh();
            }

            async function deleteCase(btn, caseId) {
                const c = casesById[caseId] || {};
                const aviso = (c.status === 'processing' || c.status === 'queued')
                    ? ' OJO: todavía se está procesando; las sub-tareas pendientes se descartarán.'
                    : '';
                if (!confirm(`¿Eliminar el caso "${c.case_name || ''}" (${caseId})? Se borran sus sub-tareas, archivos y resultados.${aviso}`)) return;
                btn.disabled = true;
                const r = await fetch(`/api/cases/${caseId}`, { method: 'DELETE' });
                if (!r.ok) {
                    alert('No se pudo eliminar el caso: ' + (await r.text()));
                    btn.disabled = false;
                    return;
                }
                if (detailCaseId === caseId) {
                    detailCaseId = null;
                    document.getElementById('case-detail').style.display = 'none';
                }
                const sel = document.getElementById('trace-case');
                if (sel.value === caseId) sel.value = '';
                traceKey = '';
                refresh();
            }

            let detailCaseId = null;

            async function showCase(caseId) {
                detailCaseId = caseId;
                await renderCaseDetail(caseId);
                // Enfocar la trazabilidad en este caso
                document.getElementById('trace-case').value = caseId;
                traceKey = '';
                refreshTrace();
            }

            async function pauseCase(btn, caseId, accion) {
                btn.disabled = true;
                const r = await fetch(`/api/cases/${caseId}/${accion}`, { method: 'POST' });
                if (!r.ok) alert('No se pudo ' + (accion === 'pause' ? 'pausar' : 'reanudar') + ': ' + (await r.text()));
                traceKey = '';
                refresh();
            }

            async function cancelCase(btn, caseId) {
                const c = casesById[caseId] || {};
                if (!confirm(`¿Cancelar el caso "${c.case_name || ''}" (${caseId})? Las sub-tareas que no terminaron no se procesarán.`)) return;
                btn.disabled = true;
                const r = await fetch(`/api/cases/${caseId}/cancel`, { method: 'POST' });
                if (!r.ok) alert('No se pudo cancelar: ' + (await r.text()));
                traceKey = '';
                refresh();
            }

            async function renderCaseDetail(caseId) {
                const data = await (await fetch(`/api/cases/${caseId}`)).json();
                if (!data.case) return;
                document.getElementById('case-detail').style.display = 'block';
                document.getElementById('detail-case-id').textContent = caseId;
                document.getElementById('report-link').href = `/cases/${caseId}/reporte`;
                document.getElementById('report-json').href = `/api/cases/${caseId}/report?download=true`;
                fetch(`/api/cases/${caseId}/report`).then(r => r.json()).then(rep => {
                    document.getElementById('detail-summary').textContent = rep.summary || '';
                });
                document.querySelector('#subtasks-table tbody').innerHTML = data.subtasks.map(s => `
                    <tr>
                        <td>${s.subtask_id}</td>
                        <td>${s.file_name}</td>
                        <td>${esc(opText(s))}</td>
                        <td>${s.pool ? `<span class="pool">${s.pool}</span>` : '-'}</td>
                        <td><span class="status ${s.status}">${STATUS_LABEL[s.status] || s.status}${
                            ['assigned', 'processing'].includes(s.status) && s.progress > 0 ? ` ${Math.round(s.progress)}%` : ''}</span>${
                            s.retries ? ` <small title="Reintentos por errores transitorios">${s.retries} reintento(s)</small>` : ''}${
                            s.reassignments ? ` <small title="Veces que pasó a otro worker porque el anterior se cayó">redistribuida</small>` : ''}</td>
                        <td>${s.assigned_worker || '-'}</td>
                        <td>${s.started_at && s.finished_at ? fmtDur(Date.parse(s.finished_at) - Date.parse(s.started_at)) : '-'}</td>
                        <td>${s.status === 'completed' && s.result_path
                              ? `<a href="/api/results/${s.subtask_id}" style="color:var(--primary-dark)">Descargar</a>`
                              : (s.error_message ? `<span title="${esc(s.error_message)}" style="color:var(--critical)">Ver error</span>` : '-')}${
                              infoText(s) ? `<br><small style="color:var(--muted)">${esc(infoText(s))}</small>` : ''}</td>
                    </tr>
                `).join('');
            }

            // ===== Variedad de archivos =====
            const TYPE_INFO = {
                video:   { label: 'Video',  color: 'var(--t-video)' },
                audio:   { label: 'Audio',  color: 'var(--t-audio)' },
                image:   { label: 'Imagen', color: 'var(--t-image)' },
                unknown: { label: 'Otro',   color: 'var(--t-other)' }
            };
            const TYPE_ORDER = ['video', 'audio', 'image', 'unknown'];
            const typeInfo = t => TYPE_INFO[t] || { label: t, color: 'var(--t-other)' };
            const attr = s => esc(s).replace(/"/g, '&quot;');

            function fmtBytes(b) {
                if (b == null) return '-';
                const u = ['B', 'KB', 'MB', 'GB'];
                let i = 0;
                while (b >= 1024 && i < u.length - 1) { b /= 1024; i++; }
                return (i === 0 ? b : b.toFixed(b < 10 ? 1 : 0)) + ' ' + u[i];
            }
            const plural = (n, s, p) => `${n} ${n === 1 ? s : p}`;

            let varietyKey = '';
            async function refreshVariety() {
                const d = await (await fetch('/api/dataset')).json();
                const key = JSON.stringify(d);
                if (key === varietyKey) return;
                varietyKey = key;
                renderVariety(d);
            }

            // Los archivos grandes son pocos (barra baja) pero pesan mucho: se resalta su volumen
            function pesadosNota(d) {
                const grandes = d.size_buckets.slice(4);              // 200 MB en adelante
                const n = grandes.reduce((a, b) => a + Object.values(b.by_type).reduce((x, y) => x + y, 0), 0);
                if (!n) return '';
                const bytes = grandes.reduce((a, b) => a + (b.bytes || 0), 0);
                const pct = d.bytes ? Math.round(bytes / d.bytes * 100) : 0;
                return `<p class="hint" style="margin:10px 0 0">${plural(n, 'archivo', 'archivos')} de más de 200 MB: ` +
                       `<b style="color:var(--text)">${fmtBytes(bytes)}</b>, el ${pct}% del volumen total.</p>`;
            }

            function renderVariety(d) {
                const types = TYPE_ORDER.filter(t => d.by_type[t]).concat(
                    Object.keys(d.by_type).filter(t => !TYPE_ORDER.includes(t)));

                document.getElementById('variety-legend').innerHTML = types.map(t =>
                    `<span><i style="background:${typeInfo(t).color}"></i>${typeInfo(t).label}</span>`).join('');

                if (!d.files) {
                    document.getElementById('variety-kpis').innerHTML = '';
                    ['chart-types', 'chart-formats', 'chart-sizes'].forEach(id =>
                        document.getElementById(id).innerHTML = '<div class="empty">Todavía no hay archivos.</div>');
                    document.querySelector('#variety-table tbody').innerHTML = '';
                    return;
                }

                document.getElementById('variety-kpis').innerHTML = [
                    ['Archivos', d.files],
                    ['Volumen total', fmtBytes(d.bytes)],
                    ['Formatos distintos', d.by_format.length],
                    ['Tamaño mínimo', fmtBytes(d.min)],
                    ['Tamaño mediano', fmtBytes(d.median)],
                    ['Tamaño máximo', fmtBytes(d.max)]
                ].map(([l, v]) => `<div class="kpi"><div class="label">${l}</div><div class="value">${v}</div></div>`).join('');

                // Archivos por tipo (barras horizontales)
                const maxType = Math.max(...types.map(t => d.by_type[t].count));
                document.getElementById('chart-types').innerHTML = types.map(t => {
                    const v = d.by_type[t], info = typeInfo(t);
                    const tip = `<b>${info.label}</b><br>${plural(v.count, 'archivo', 'archivos')} · ${fmtBytes(v.bytes)}<br>${Math.round(v.count / d.files * 100)}% del total`;
                    return `<div class="hbar-row" data-tip="${attr(tip)}">
                        <span class="name">${info.label}</span>
                        <div class="hbar-track"><div class="hbar" style="width:${v.count / maxType * 100}%;background:${info.color}"></div></div>
                        <span class="hbar-val">${v.count} · ${fmtBytes(v.bytes)}</span></div>`;
                }).join('');

                // Formatos (barras horizontales, color por tipo)
                const formats = d.by_format.slice().sort((a, b) =>
                    TYPE_ORDER.indexOf(a.file_type) - TYPE_ORDER.indexOf(b.file_type) || b.count - a.count);
                const maxFmt = Math.max(...formats.map(f => f.count));
                document.getElementById('chart-formats').innerHTML = formats.map(f => {
                    const info = typeInfo(f.file_type);
                    const tip = `<b>.${esc(f.format)}</b> (${info.label})<br>${plural(f.count, 'archivo', 'archivos')} · ${fmtBytes(f.bytes)}`;
                    return `<div class="hbar-row" data-tip="${attr(tip)}">
                        <span class="name">${esc(f.format.toUpperCase())}</span>
                        <div class="hbar-track"><div class="hbar" style="width:${f.count / maxFmt * 100}%;background:${info.color}"></div></div>
                        <span class="hbar-val">${f.count}</span></div>`;
                }).join('');

                // Distribución de tamaños (columnas apiladas por tipo)
                const totals = d.size_buckets.map(b => Object.values(b.by_type).reduce((a, c) => a + c, 0));
                const maxBucket = Math.max(1, ...totals);
                const plotH = 120;
                const sinMedir = d.files - d.measured;
                document.getElementById('chart-sizes').innerHTML =
                    (sinMedir ? `<p class="hint">${plural(sinMedir, 'archivo no tiene', 'archivos no tienen')} tamaño registrado.</p>` : '') +
                    `<div class="cols">` + d.size_buckets.map((b, i) => {
                        const segs = types.filter(t => b.by_type[t]).map(t => {
                            const n = b.by_type[t], info = typeInfo(t);
                            const tip = `<b>${b.label}</b><br>${info.label}: ${plural(n, 'archivo', 'archivos')}`;
                            return `<div class="seg" data-tip="${attr(tip)}" style="height:${Math.max(2, n / maxBucket * plotH)}px;background:${info.color}"></div>`;
                        }).join('');
                        return `<div class="col"><div class="total">${totals[i] || ''}</div><div class="stack">${segs}</div></div>`;
                    }).join('') + `</div>` +
                    `<div class="col-labels">${d.size_buckets.map(b => `<div>${b.label}</div>`).join('')}</div>` +
                    pesadosNota(d);

                // Tabla
                document.querySelector('#variety-table tbody').innerHTML = formats.map(f => `
                    <tr><td>${esc(f.format.toUpperCase())}</td><td>${typeInfo(f.file_type).label}</td>
                        <td>${f.count}</td><td>${fmtBytes(f.bytes)}</td></tr>`).join('');
            }

            // Tooltips de los gráficos de variedad
            const varietyEl = document.querySelector('.variety');
            varietyEl.addEventListener('mousemove', ev => {
                const el = ev.target.closest('[data-tip]');
                if (el) showTip(el.dataset.tip, ev); else hideTip();
            });
            varietyEl.addEventListener('mouseleave', hideTip);

            // ===== Trazabilidad =====
            const NS = 'http://www.w3.org/2000/svg';
            const OP_LABEL = {
                convert_format: 'Convertir', extract_audio: 'Extraer audio',
                extract_metadata: 'Metadatos', generate_thumbnail: 'Miniatura',
                unsupported: 'No soportado'
            };
            OP_LABEL.generate_thumbnail_video = 'Portada';
            const STATUS_LABEL = { completed: 'completada', failed: 'fallida', processing: 'en proceso', assigned: 'asignada',
                                   pending: 'en cola', retrying: 'reintentando', cancelled: 'cancelada', paused: 'en pausa' };
            const QUEUE = '__cola__';
            const tooltip = document.getElementById('tooltip');

            // Color fijo por worker: se asigna la primera vez que aparece y no cambia
            const workerColor = {};
            let nextSlot = 1;
            function colorFor(w) {
                if (!w || w === QUEUE) return 'var(--queue)';
                if (!workerColor[w]) workerColor[w] = nextSlot <= 8 ? `var(--w${nextSlot++})` : 'var(--w-other)';
                return workerColor[w];
            }

            function el(tag, attrs, text) {
                const e = document.createElementNS(NS, tag);
                for (const k in attrs) e.setAttribute(k, attrs[k]);
                if (text != null) e.textContent = text;
                return e;
            }
            function trunc(s, n) { return s.length > n ? s.slice(0, Math.max(1, n - 1)) + '…' : s; }
            function opText(s) {
                const k = s.operation === 'generate_thumbnail' && s.file_type === 'video' ? 'generate_thumbnail_video' : s.operation;
                const tf = s.target_format && !['extract_metadata', 'generate_thumbnail'].includes(s.operation) ? ' → ' + s.target_format : '';
                return (OP_LABEL[k] || s.operation) + tf;
            }
            function esc(s) { const d = document.createElement('div'); d.textContent = s == null ? '' : s; return d.innerHTML; }
            function fmtDur(ms) {
                const s = ms / 1000;
                if (s < 60) return s.toFixed(s < 10 ? 1 : 0) + ' s';
                const m = Math.floor(s / 60), r = Math.round(s % 60);
                return `${m} min ${String(r).padStart(2, '0')} s`;
            }
            function showTip(html, ev) {
                tooltip.innerHTML = html;
                tooltip.style.display = 'block';
                const r = tooltip.getBoundingClientRect();
                let x = ev.clientX + 14, y = ev.clientY + 14;
                if (x + r.width > window.innerWidth - 8) x = ev.clientX - r.width - 14;
                if (y + r.height > window.innerHeight - 8) y = ev.clientY - r.height - 14;
                tooltip.style.left = x + 'px';
                tooltip.style.top = y + 'px';
            }
            function hideTip() { tooltip.style.display = 'none'; }
            function taskTip(s, now) {
                const st = s.started_at ? Date.parse(s.started_at) : null;
                const en = s.finished_at ? Date.parse(s.finished_at) : (s.status === 'processing' ? now : null);
                return `<b>${esc(s.file_name)}</b><br>${esc(opText(s))}<br>` +
                       `Estado: ${STATUS_LABEL[s.status] || esc(s.status)}<br>` +
                       `Worker: ${s.assigned_worker ? `<i style="background:${colorFor(s.assigned_worker)}"></i>${esc(s.assigned_worker)}` : 'esperando en la cola'}` +
                       (st && en ? `<br>Duración: ${fmtDur(en - st)}` : '') +
                       (s.error_message ? `<br>Error: ${esc(trunc(s.error_message, 120))}` : '');
            }

            let traceKey = '';
            let lastTrace = null;

            async function refreshTrace() {
                const sel = document.getElementById('trace-case');
                const q = sel.value ? '?case_id=' + encodeURIComponent(sel.value) : '';
                const data = await (await fetch('/api/trace' + q)).json();
                const live = data.subtasks.some(s => s.status === 'processing');
                // Redibujar solo si algo cambió (así el hover no parpadea)
                const key = JSON.stringify([q, data.subtasks, data.workers, live ? data.now : 0]);
                if (key === traceKey) return;
                traceKey = key;
                lastTrace = data;
                renderTrace(data);
            }

            function renderTrace(data) {
                // Workers conocidos: los registrados + los que aparecen en sub-tareas
                const ids = new Set(data.workers.map(w => w.worker_id));
                data.subtasks.forEach(s => s.assigned_worker && ids.add(s.assigned_worker));
                const workers = [...ids].sort();
                workers.forEach(colorFor);

                document.getElementById('trace-legend').innerHTML = workers.map(w =>
                    `<span><i style="background:${colorFor(w)}"></i>${esc(w)}</span>`).join('');

                renderFlow(data, workers);
                renderTimeline(data, workers);
            }

            // ---------- Mapa de flujo ----------
            function renderFlow(data, workers) {
                const svg = document.getElementById('flow-map');
                svg.replaceChildren();
                svg.parentElement.querySelectorAll('.empty').forEach(n => n.remove());
                const subs = data.subtasks;
                if (!subs.length) {
                    svg.setAttribute('width', 0); svg.setAttribute('height', 0);
                    svg.parentElement.insertAdjacentHTML('beforeend', '<div class="empty">Todavía no hay sub-tareas para mostrar. Subí un caso arriba.</div>');
                    return;
                }

                const W = Math.max(svg.parentElement.clientWidth, 860);
                const top = 34, rowH = 30, caseGap = 16, nodeH = 24;
                const colX = [12, W * 0.22, W * 0.47, W - 182];
                const colW = [W * 0.22 - 36, W * 0.25 - 36, W * 0.30 - 40, 170];

                // Agrupar: caso → archivo → sub-tareas
                const cases = [];
                const caseIdx = {}, fileIdx = {};
                let y = top;
                subs.forEach(s => {
                    if (!(s.case_id in caseIdx)) {
                        if (cases.length) y += caseGap;
                        caseIdx[s.case_id] = cases.length;
                        cases.push({ id: s.case_id, name: s.case_name, files: [], subs: [] });
                    }
                    const c = cases[caseIdx[s.case_id]];
                    const fk = s.case_id + '/' + s.file_name;
                    if (!(fk in fileIdx)) {
                        fileIdx[fk] = c.files.length;
                        c.files.push({ name: s.file_name, subs: [] });
                    }
                    s._y = y + rowH / 2;
                    y += rowH;
                    c.files[fileIdx[fk]].subs.push(s);
                    c.subs.push(s);
                });
                const mean = arr => arr.reduce((a, s) => a + s._y, 0) / arr.length;

                // Nodos de worker (+ la cola si hay sub-tareas pendientes)
                const wcol = workers.filter(w => subs.some(s => s.assigned_worker === w) ||
                                                 data.workers.some(x => x.worker_id === w));
                if (subs.some(s => !s.assigned_worker)) wcol.push(QUEUE);
                const wH = 42, wGap = 14;
                const wTotal = wcol.length * wH + (wcol.length - 1) * wGap;
                const H = Math.max(y, top + wTotal) + 12;
                const wStart = top + Math.max(0, (H - 12 - top - wTotal) / 2);
                const wY = {};
                wcol.forEach((w, i) => wY[w] = wStart + i * (wH + wGap) + wH / 2);

                svg.setAttribute('width', W);
                svg.setAttribute('height', H);
                svg.setAttribute('viewBox', `0 0 ${W} ${H}`);
                svg.setAttribute('role', 'img');
                svg.setAttribute('aria-label', 'Mapa de flujo de casos, archivos, sub-tareas y workers');

                ['Caso', 'Archivo', 'Sub-tarea', 'Worker'].forEach((t, i) =>
                    svg.appendChild(el('text', { x: colX[i], y: 18, fill: '#4b8a99', 'font-size': 11, 'font-weight': 600, 'letter-spacing': '0.06em' }, t.toUpperCase())));

                const gLinks = el('g', {}), gNodes = el('g', {});
                svg.append(gLinks, gNodes);
                const items = [];   // elementos con su conjunto de sub-tareas, para resaltar
                const reg = (e, set) => { e._sts = set; items.push(e); return e; };

                function link(x1, y1, x2, y2, attrs, set) {
                    const mx = (x1 + x2) / 2;
                    const p = el('path', Object.assign({
                        d: `M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}`,
                        fill: 'none', stroke: 'var(--link)', 'stroke-width': 1.5
                    }, attrs));
                    gLinks.appendChild(reg(p, set));
                }

                function node(x, yc, w, h, label, opts) {
                    const g = el('g', { class: 'node', transform: `translate(${x},${yc - h / 2})`, style: 'cursor:default' });
                    g.appendChild(el('rect', {
                        width: w, height: h, rx: 7,
                        fill: opts.fill || '#fff', stroke: opts.stroke || 'var(--border)',
                        'stroke-width': opts.strokeW || 1
                    }));
                    let tx = 9;
                    if (opts.swatch) {
                        g.appendChild(el('rect', { x: 8, y: h / 2 - 5, width: 10, height: 10, rx: 2, fill: opts.swatch }));
                        tx = 24;
                    }
                    const maxChars = Math.floor((w - tx - 6) / 6.6);
                    g.appendChild(el('text', { x: tx, y: opts.sub ? h / 2 - 3 : h / 2 + 4, fill: '#164e63', 'font-size': 12, 'font-weight': opts.bold ? 600 : 400 }, trunc(label, maxChars)));
                    if (opts.sub) g.appendChild(el('text', { x: tx, y: h / 2 + 11, fill: '#4b8a99', 'font-size': 10.5 }, trunc(opts.sub, maxChars + 2)));
                    if (opts.title) g.appendChild(el('title', {}, opts.title));
                    reg(g, opts.set);
                    g.addEventListener('mouseenter', ev => {
                        items.forEach(it => {
                            const hit = [...it._sts].some(id => opts.set.has(id));
                            it.classList.toggle('dim', !hit);
                        });
                        if (opts.tip) showTip(opts.tip, ev);
                    });
                    g.addEventListener('mousemove', ev => opts.tip && showTip(opts.tip, ev));
                    g.addEventListener('mouseleave', () => { items.forEach(it => it.classList.remove('dim')); hideTip(); });
                    gNodes.appendChild(g);
                }

                cases.forEach(c => {
                    const cy = mean(c.subs);
                    const cset = new Set(c.subs.map(s => s.subtask_id));
                    c.files.forEach(f => {
                        const fy = mean(f.subs);
                        const fset = new Set(f.subs.map(s => s.subtask_id));
                        link(colX[0] + colW[0], cy, colX[1], fy, {}, fset);
                        f.subs.forEach(s => {
                            const one = new Set([s.subtask_id]);
                            link(colX[1] + colW[1], fy, colX[2], s._y, {}, one);
                            const w = s.assigned_worker || QUEUE;
                            const attrs = { stroke: colorFor(w), 'stroke-width': 2 };
                            if (!s.assigned_worker) Object.assign(attrs, { 'stroke-dasharray': '4 4' });
                            if (s.status === 'processing' || s.status === 'assigned') attrs.class = 'live';
                            link(colX[2] + colW[2], s._y, colX[3], wY[w], attrs, one);
                            node(colX[2], s._y, colW[2], nodeH, opText(s) + (s.status === 'completed' ? '' : ` (${STATUS_LABEL[s.status] || s.status})`), {
                                set: one, tip: taskTip(s, Date.parse(data.now)),
                                stroke: s.status === 'failed' ? 'var(--critical)' : undefined,
                                strokeW: s.status === 'failed' ? 1.5 : 1
                            });
                        });
                        const fsub = f.subs.length === 1 ? '1 sub-tarea' : `${f.subs.length} sub-tareas`;
                        node(colX[1], fy, colW[1], f.subs.length > 1 ? 34 : nodeH, f.name, {
                            set: fset, title: f.name, sub: f.subs.length > 1 ? fsub : null
                        });
                    });
                    node(colX[0], cy, colW[0], 36, c.name, { set: cset, fill: '#ecfeff', bold: true, sub: c.id, title: `${c.name} (${c.id})` });
                });

                wcol.forEach(w => {
                    const mine = subs.filter(s => (s.assigned_worker || QUEUE) === w);
                    const n = mine.length;
                    const label = w === QUEUE ? 'Cola RabbitMQ' : w;
                    const sub = w === QUEUE ? `${n} esperando` : (n === 1 ? '1 sub-tarea' : `${n} sub-tareas`);
                    node(colX[3], wY[w], colW[3], wH, label, {
                        set: new Set(mine.map(s => s.subtask_id)), swatch: colorFor(w), bold: true, sub,
                        stroke: colorFor(w), strokeW: 2, title: label
                    });
                });
            }

            // ---------- Línea de tiempo (Gantt por worker) ----------
            function renderTimeline(data, workers) {
                const svg = document.getElementById('timeline');
                const wrap = svg.parentElement;
                svg.replaceChildren();
                wrap.querySelectorAll('.empty').forEach(n => n.remove());

                const now = Date.parse(data.now);
                const tasks = data.subtasks.filter(s => s.started_at && s.assigned_worker).map(s => ({
                    s, w: s.assigned_worker,
                    a: Date.parse(s.started_at),
                    b: s.finished_at ? Date.parse(s.finished_at) : now
                }));
                tasks.forEach(t => { if (t.b < t.a) t.b = t.a; });

                const kpis = document.getElementById('trace-kpis');
                if (!tasks.length) {
                    svg.setAttribute('width', 0); svg.setAttribute('height', 0);
                    kpis.innerHTML = '';
                    wrap.insertAdjacentHTML('beforeend', '<div class="empty">Todavía no hay sub-tareas iniciadas. (Los workers tienen que tener la versión nueva de worker.py para reportar cuándo empiezan.)</div>');
                    return;
                }

                const t0 = Math.min(...tasks.map(t => t.a));
                const t1 = Math.max(...tasks.map(t => t.b));
                const span = Math.max(t1 - t0, 1000);

                // Métricas de paralelismo
                const events = [];
                tasks.forEach(t => { events.push([t.a, 1], [t.b, -1]); });
                events.sort((x, y) => x[0] - y[0] || x[1] - y[1]);
                let cur = 0, maxPar = 0;
                events.forEach(e => { cur += e[1]; maxPar = Math.max(maxPar, cur); });
                const busy = tasks.reduce((a, t) => a + (t.b - t.a), 0);
                const speedup = busy / span;
                const usedWorkers = new Set(tasks.map(t => t.w)).size;
                kpis.innerHTML = [
                    ['Workers que participaron', usedWorkers],
                    ['Máx. tareas en paralelo', maxPar],
                    ['Tiempo real (paralelo)', fmtDur(span)],
                    ['Tiempo si fuera secuencial', fmtDur(busy)],
                    ['Aceleración (speedup)', speedup.toFixed(2) + '×']
                ].map(([l, v]) => `<div class="kpi"><div class="label">${l}</div><div class="value">${v}</div></div>`).join('');

                const lanes = workers.filter(w => tasks.some(t => t.w === w) ||
                                                  data.workers.some(x => x.worker_id === w));
                const W = Math.max(wrap.clientWidth, 860);
                const labelW = 150, right = 20, top = 10, laneH = 38, axisH = 28;
                const plotW = W - labelW - right;
                const H = top + lanes.length * laneH + axisH;
                const X = t => labelW + (t - t0) / span * plotW;

                svg.setAttribute('width', W);
                svg.setAttribute('height', H);
                svg.setAttribute('viewBox', `0 0 ${W} ${H}`);
                svg.setAttribute('role', 'img');
                svg.setAttribute('aria-label', 'Línea de tiempo de sub-tareas por worker');

                // Grilla y eje (segundos desde el primer inicio)
                const steps = [0.5, 1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 1800, 3600];
                const step = (steps.find(s => span / 1000 / s <= 8) || 3600) * 1000;
                const axisY = top + lanes.length * laneH;
                for (let t = 0; t <= span + 1; t += step) {
                    const x = X(t0 + t);
                    svg.appendChild(el('line', { x1: x, x2: x, y1: top, y2: axisY, stroke: 'var(--grid)' }));
                    svg.appendChild(el('text', { x, y: axisY + 17, 'text-anchor': 'middle', fill: '#4b8a99', 'font-size': 11 },
                        fmtDur(t)));
                }
                svg.appendChild(el('line', { x1: labelW, x2: labelW + plotW, y1: axisY, y2: axisY, stroke: '#a5d8e3' }));

                const bars = [];
                lanes.forEach((w, i) => {
                    const ly = top + i * laneH;
                    if (i % 2 === 0) svg.appendChild(el('rect', { x: 0, y: ly, width: W, height: laneH, fill: '#f6fdff' }));
                    svg.appendChild(el('rect', { x: 10, y: ly + laneH / 2 - 5, width: 10, height: 10, rx: 2, fill: colorFor(w) }));
                    svg.appendChild(el('text', { x: 26, y: ly + laneH / 2 + 4, fill: '#164e63', 'font-size': 12 }, trunc(w, 18)));

                    tasks.filter(t => t.w === w).forEach(t => {
                        const x = X(t.a), bw = Math.max(X(t.b) - x, 3);
                        const failed = t.s.status === 'failed';
                        const r = el('rect', {
                            x, y: ly + 8, width: bw, height: laneH - 16, rx: 4,
                            fill: colorFor(w), 'fill-opacity': failed ? 0.35 : 1,
                            stroke: failed ? 'var(--critical)' : '#fff', 'stroke-width': failed ? 1.5 : 1
                        });
                        if (t.s.status === 'processing') r.setAttribute('class', 'pulse');
                        svg.appendChild(r);
                        const label = t.s.file_name + (failed ? ' (fallida)' : '');
                        const chars = Math.floor((bw - 10) / 6.4);
                        if (chars >= 5) svg.appendChild(el('text', {
                            x: x + 6, y: ly + laneH / 2 + 4, 'font-size': 11,
                            fill: failed ? '#7f1d1d' : '#fff', 'pointer-events': 'none'
                        }, trunc(label, chars)));
                        bars.push({ r, t });
                    });
                });

                // Cursor vertical: muestra qué corría en paralelo en ese instante
                const cursor = el('line', { y1: top, y2: axisY, stroke: '#0e7490', 'stroke-width': 1, 'stroke-dasharray': '3 3', visibility: 'hidden' });
                svg.appendChild(cursor);
                const hit = el('rect', { x: labelW, y: top, width: plotW, height: axisY - top, fill: 'transparent' });
                svg.appendChild(hit);
                hit.addEventListener('mousemove', ev => {
                    const pt = svg.getBoundingClientRect();
                    const px = (ev.clientX - pt.left) * (W / pt.width);
                    const t = t0 + (px - labelW) / plotW * span;
                    cursor.setAttribute('x1', px); cursor.setAttribute('x2', px);
                    cursor.setAttribute('visibility', 'visible');
                    const active = bars.filter(b => b.t.a <= t && t <= b.t.b);
                    bars.forEach(b => b.r.classList.toggle('dim', !active.includes(b)));
                    const list = active.map(b =>
                        `<i style="background:${colorFor(b.t.w)}"></i>${esc(b.t.w)}: ${esc(trunc(b.t.s.file_name, 28))} · ${esc(opText(b.t.s))}`).join('<br>');
                    showTip(`<b>t = ${fmtDur(t - t0)}</b> · ${active.length} en paralelo` + (list ? '<br>' + list : ''), ev);
                });
                hit.addEventListener('mouseleave', () => {
                    cursor.setAttribute('visibility', 'hidden');
                    bars.forEach(b => b.r.classList.remove('dim'));
                    hideTip();
                });
            }

            document.getElementById('trace-case').addEventListener('change', () => { traceKey = ''; refreshTrace(); });
            let resizeTimer;
            window.addEventListener('resize', () => {
                clearTimeout(resizeTimer);
                resizeTimer = setTimeout(() => lastTrace && renderTrace(lastTrace), 150);
            });

            // Refrescar cada 3 segundos
            refresh();
            setInterval(refresh, 3000);
        </script>
    </body>
    </html>
    """


# ============================================================
# ARRANQUE
# ============================================================
if __name__ == "__main__":
    ensure_schema()
    ensure_long_task_policy()

    # Iniciar hilo que escucha resultados (barrier/join)
    result_thread = threading.Thread(target=listen_results, daemon=True)
    result_thread.start()

    print("=" * 50)
    print("COORDINADOR iniciado")
    port = int(os.getenv("PORT", "8000"))
    print(f"Dashboard: http://localhost:{port}")
    print(f"API docs:  http://localhost:{port}/docs")
    if QUEUE_PREFIX:
        print(f"Colas con prefijo: {QUEUE_PREFIX}")
    print("=" * 50)

    uvicorn.run(app, host="0.0.0.0", port=port)
