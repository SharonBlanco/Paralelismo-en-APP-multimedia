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
  CPU_HIGH / CPU_LOW   umbrales (%) para pausar / reanudar       [80 / 60]

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
import re
import json
import time
import socket
import platform
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
CPU_HIGH     = float(os.getenv("CPU_HIGH", "80"))
CPU_LOW      = float(os.getenv("CPU_LOW", "60"))

# Prefijo opcional de colas (para pruebas aisladas); debe coincidir con el coordinador
QUEUE_PREFIX  = os.getenv("QUEUE_PREFIX", "")
POOL_QUEUES   = {p: f"{QUEUE_PREFIX}tareas.{p}" for p in ALL_POOLS}
RESULTS_QUEUE = f"{QUEUE_PREFIX}results"
QUEUE_ARGS    = {"x-max-priority": 10}   # cola con prioridades (1 = baja, 10 = alta)

# Versión del worker: el coordinador avisa si un worker está desactualizado
WORKER_VERSION = 5

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


class Paused(Exception):
    """El coordinador avisó que el caso está en pausa"""


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
        if r.status_code == 423:
            raise Paused()
        r.raise_for_status()
        with open(local_path, "wb") as fp:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                fp.write(chunk)
    return local_path


def upload_result(subtask_id: str, out_file: str) -> str:
    """
    Sube el archivo resultante al coordinador y devuelve su ruta allá.
    Se envía como flujo (no se carga entero en memoria): un video
    convertido puede pesar cientos de MB.
    """
    with open(out_file, "rb") as fp:
        r = requests.post(
            f"{COORDINATOR_URL}/api/results/{quote(subtask_id)}/raw",
            params={"filename": Path(out_file).name},
            data=fp,
            headers={"Content-Type": "application/octet-stream"},
            timeout=(10, 600)
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
# PORTADA DE VIDEO CON CRITERIO
# ============================================================
def best_video_frame(file_path: str, duration: float, out_file: str, subtask_id: str) -> dict:
    """
    Elige la portada de un video en dos pasos:
      1. Toma 5 ventanas repartidas a lo largo del video (10 %, 30 %, 50 %,
         70 % y 90 %). En cada una, el filtro `thumbnail` de FFmpeg analiza
         60 cuadros y se queda con el más representativo (el más parecido al
         histograma promedio), descartando cuadros de transición o borrosos.
      2. Entre esos 5 candidatos elige el de más detalle visual, medido como
         el tamaño del JPEG a igual resolución y calidad: un cuadro negro,
         en blanco o liso comprime muchísimo; uno con contenido, no.
    """
    posiciones = [0.1, 0.3, 0.5, 0.7, 0.9] if duration and duration > 5 else [0.0]
    candidatos = []
    for i, frac in enumerate(posiciones):
        t = (duration or 0) * frac
        cand = f"{out_file}.cand{i}.jpg"
        try:
            run_ffmpeg(["-ss", f"{t:.2f}", "-t", "4", "-i", file_path,
                        "-vf", "thumbnail=60,scale=480:-2", "-frames:v", "1", "-q:v", "3", cand])
            candidatos.append((Path(cand).stat().st_size, t, cand))
        except (subprocess.CalledProcessError, OSError):
            pass
        report_progress(subtask_id, (i + 1) / len(posiciones) * 90)
    if not candidatos:
        raise subprocess.CalledProcessError(1, "thumbnail", stderr="No se pudo extraer ningún cuadro del video")

    peso, segundo, elegido = max(candidatos)
    shutil.move(elegido, out_file)
    for _, _, c in candidatos:
        Path(c).unlink(missing_ok=True)
    return {
        "segundo": round(segundo, 1),
        "candidatos": len(candidatos),
        "criterio": "cuadro más representativo de 5 ventanas (filtro thumbnail) y de mayor detalle",
    }


# ============================================================
# METADATOS: técnicos (ffprobe) + asociados (MusicBrainz) + letra
# ============================================================
MB_HEADERS = {"User-Agent": "ProyectoSO-TEC-worker/4 (https://github.com/SharonBlanco/Proyecto)"}
_mb_lock = threading.Lock()
_mb_last = [0.0]
VERSION_WORDS = {
    "acústica": ["acoustic", "acústic", "acustic", "unplugged"],
    "en vivo": ["live", "en vivo", "concert"],
    "remix": ["remix", "mix)"],
    "instrumental": ["instrumental", "karaoke"],
    "demo": ["demo"],
}


def detect_version(*texts):
    t = " ".join(x for x in texts if x).lower()
    for nombre, palabras in VERSION_WORDS.items():
        if any(p in t for p in palabras):
            return nombre
    return "original"


def musicbrainz_lookup(title: str, artist: str = None) -> dict:
    """
    Busca la grabación en MusicBrainz (base de datos musical abierta) y
    devuelve álbum, fecha de lanzamiento, duración y enlace. MusicBrainz
    pide como máximo 1 consulta por segundo: se respeta con un candado.
    """
    base = f'recording:"{title}"' + (f' AND artist:"{artist}"' if artist else "")
    # Primero solo lanzamientos oficiales de tipo álbum (sin en vivo ni
    # recopilatorios); si no hay resultados, cualquier lanzamiento.
    consultas = [base + " AND status:official AND primarytype:album"
                        " AND NOT secondarytype:live AND NOT secondarytype:compilation", base]
    recs = []
    for query in consultas:
        for intento in range(4):
            with _mb_lock:
                espera = 1.2 - (time.time() - _mb_last[0])
                if espera > 0:
                    time.sleep(espera)
                _mb_last[0] = time.time()
                r = requests.get("https://musicbrainz.org/ws/2/recording",
                                 params={"query": query, "fmt": "json", "limit": 25},
                                 headers=MB_HEADERS, timeout=10)
            if r.status_code != 503:          # 503 = demasiadas consultas: esperar y reintentar
                break
            time.sleep(2 * (intento + 1))
        r.raise_for_status()
        recs = [x for x in r.json().get("recordings", []) if int(x.get("score", 0)) >= 80]
        if recs:
            break
    if not recs:
        return {"encontrado": False, "fuente": "MusicBrainz"}

    # Una canción tiene muchas grabaciones y lanzamientos. Se agrupan por
    # título de álbum y se elige el álbum oficial original: oficial, de tipo
    # álbum, sin tipo secundario, el que más veces aparece (el original
    # suele tener muchas reediciones) y, a igualdad, el más antiguo.
    mejor = int(recs[0].get("score", 0))
    albumes = {}
    for rec in recs:
        if int(rec.get("score", 0)) < mejor - 5:
            continue
        for rel in rec.get("releases", []):
            rg = rel.get("release-group", {})
            a = albumes.setdefault(rel.get("title"), {"rel": rel, "rec": rec, "veces": 0,
                                                      "fecha": rel.get("date") or "9999"})
            a["veces"] += 1
            if (rel.get("date") or "9999") < a["fecha"]:
                a.update(rel=rel, rec=rec, fecha=rel.get("date"))
            a["clave"] = (rel.get("status") != "Official", rg.get("primary-type") != "Album",
                          bool(rg.get("secondary-types")))
    if albumes:
        elegido = min(albumes.values(), key=lambda a: (a["clave"], -a["veces"], a["fecha"]))
        rel, rec = elegido["rel"], elegido["rec"]
    else:
        rel, rec = {}, recs[0]
    return {
        "encontrado": True,
        "fuente": "MusicBrainz",
        "titulo": rec.get("title"),
        "artista": ", ".join(a.get("name", "") for a in rec.get("artist-credit", []) if isinstance(a, dict)),
        "album": rel.get("title"),
        "fecha": rec.get("first-release-date") or rel.get("date"),
        "pais": rel.get("country"),
        "duracion_s": round(rec["length"] / 1000) if rec.get("length") else None,
        "desambiguacion": rec.get("disambiguation") or None,
        "coincidencia": int(rec.get("score", 0)),
        "enlace": f"https://musicbrainz.org/recording/{rec['id']}",
    }


_it_lock = threading.Lock()
_it_last = [0.0]


def _normalizar(t):
    t = re.sub(r"\(.*?\)|\[.*?\]", "", (t or "").lower())
    return re.sub(r"[^a-z0-9áéíóúñü ]", "", t).strip()


def itunes_lookup(title: str, artist: str = None) -> dict:
    """
    Busca la canción en la API pública de búsqueda de iTunes (sin clave):
    álbum, fecha, género, duración, número de pista y carátula. Es más
    precisa que MusicBrainz para música popular. Permite ~20 consultas
    por minuto: se espacian con un candado.
    """
    with _it_lock:
        espera = 3.1 - (time.time() - _it_last[0])
        if espera > 0:
            time.sleep(espera)
        _it_last[0] = time.time()
        r = requests.get("https://itunes.apple.com/search",
                         params={"term": f"{artist or ''} {title}".strip(), "entity": "song", "limit": 10},
                         headers=MB_HEADERS, timeout=10)
    r.raise_for_status()                      # 403/429 = límite de consultas → se usa MusicBrainz
    t_norm, a_norm = _normalizar(title), _normalizar(artist)
    for x in r.json().get("results", []):
        if t_norm and t_norm not in _normalizar(x.get("trackName")):
            continue
        if a_norm and a_norm.split()[0] not in _normalizar(x.get("artistName")):
            continue
        return {
            "encontrado": True,
            "fuente": "iTunes",
            "titulo": x.get("trackName"),
            "artista": x.get("artistName"),
            "album": x.get("collectionName"),
            "fecha": (x.get("releaseDate") or "")[:10] or None,
            "genero": x.get("primaryGenreName"),
            "duracion_s": round(x["trackTimeMillis"] / 1000) if x.get("trackTimeMillis") else None,
            "pista": f'{x.get("trackNumber")}/{x.get("trackCount")}' if x.get("trackNumber") else None,
            "caratula": (x.get("artworkUrl100") or "").replace("100x100", "600x600") or None,
            "enlace": x.get("trackViewUrl"),
        }
    return {"encontrado": False, "fuente": "iTunes"}


def lyrics_lookup(artist: str, title: str):
    """Letra de la canción desde lyrics.ovh (servicio abierto); None si no hay"""
    if not artist or not title:
        return None
    r = requests.get(f"https://api.lyrics.ovh/v1/{quote(artist)}/{quote(title)}", timeout=8)
    if r.status_code != 200:
        return None
    letra = (r.json().get("lyrics") or "").strip()
    return letra[:6000] or None


def build_metadata(file_path: str, file_name: str, out_file: str) -> dict:
    """Genera el JSON de metadatos y devuelve un resumen para el reporte"""
    probe = json.loads(subprocess.run(
        ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", "-show_streams", file_path],
        capture_output=True, text=True, check=True).stdout or "{}")
    fmt = probe.get("format", {})
    tags = {k.lower(): v for k, v in (fmt.get("tags") or {}).items()}
    audio = next((st for st in probe.get("streams", []) if st.get("codec_type") == "audio"), {})

    titulo = tags.get("title") or Path(file_name).stem.replace("_", " ")
    artista = tags.get("artist") or tags.get("album_artist")
    tecnicos = {
        "duracion_s": round(float(fmt.get("duration", 0) or 0), 1),
        "bitrate_kbps": round(int(fmt.get("bit_rate", 0) or 0) / 1000),
        "codec": audio.get("codec_name"),
        "frecuencia_hz": int(audio.get("sample_rate", 0) or 0) or None,
        "canales": audio.get("channels"),
        "tamano_bytes": int(fmt.get("size", 0) or 0),
    }

    # Datos asociados (consulta externa): iTunes y, si no encuentra o limita
    # las consultas, MusicBrainz. Si no hay internet, la sub-tarea no falla:
    # el JSON indica que no se pudo consultar.
    asociados = {"encontrado": False}
    for buscar in (itunes_lookup, musicbrainz_lookup):
        try:
            asociados = buscar(titulo, artista)
            if not asociados["encontrado"] and artista:
                asociados = buscar(titulo)                   # segundo intento solo por título
        except requests.RequestException as e:
            asociados = {"encontrado": False, "fuente": buscar.__name__, "error": str(e)[:200]}
        if asociados["encontrado"]:
            break
    try:
        letra = lyrics_lookup(asociados.get("artista") or artista, asociados.get("titulo") or titulo)
    except requests.RequestException:
        letra = None

    version = detect_version(titulo, tags.get("album"), asociados.get("desambiguacion"), asociados.get("titulo"))
    data = {
        "archivo": file_name,
        "etiquetas": tags,
        "tecnicos": tecnicos,
        "asociados": asociados,
        "version": version,
        "letra": letra,
    }
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return {
        "titulo": asociados.get("titulo") or titulo,
        "artista": asociados.get("artista") or artista,
        "album": asociados.get("album") or tags.get("album"),
        "fecha": asociados.get("fecha") or tags.get("date"),
        "duracion_s": tecnicos["duracion_s"],
        "version": version,
        "genero": asociados.get("genero"),
        "letra": bool(letra),
        "fuente": asociados.get("fuente") if asociados.get("encontrado") else None,
    }


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
        "info": None,       # resumen para el reporte (portada elegida, metadatos, tamaños)
    }

    try:
        if operation == "unsupported":
            raise ValueError(f"Formato no soportado: {Path(file_name).suffix or '(sin extensión)'}")

        file_path = str(download_input(subtask))
        notify("processing")
        duration = probe_duration(file_path) if file_type in ("video", "audio") else None

        if operation == "convert_format":
            out_file = f"{output_path}.{target_fmt}"
            args = ["-i", file_path]
            if file_type == "video":
                # H.264 con preset rápido: transcodificación real pero en tiempo razonable
                args += ["-c:v", "libx264", "-preset", "veryfast", "-crf", "23", "-c:a", "aac", "-b:a", "160k"]
            run_ffmpeg(args + [out_file], subtask_id, duration)
            result["info"] = {"entrada_mb": round(Path(file_path).stat().st_size / 2**20, 1),
                              "salida_mb": round(Path(out_file).stat().st_size / 2**20, 1),
                              "duracion_s": round(duration or 0, 1)}
            print(f"    [✓] Convertido: {file_name} → .{target_fmt}")

        elif operation == "extract_audio":
            out_file = f"{output_path}.mp3"
            run_ffmpeg(["-i", file_path, "-vn", "-acodec", "libmp3lame", out_file], subtask_id, duration)
            print(f"    [✓] Audio extraído: {file_name} → .mp3")

        elif operation == "generate_thumbnail":
            if file_type == "video":
                out_file = f"{output_path}_portada.jpg"
                result["info"] = best_video_frame(file_path, duration, out_file, subtask_id)
                print(f"    [✓] Portada: {file_name} (segundo {result['info']['segundo']})")
            else:
                out_file = f"{output_path}_thumb.jpg"
                run_ffmpeg(["-i", file_path, "-vf", "scale=320:-1", out_file])
                print(f"    [✓] Miniatura: {file_name}")

        elif operation == "extract_metadata":
            out_file = f"{output_path}_metadata.json"
            result["info"] = build_metadata(file_path, file_name, out_file)
            encontrado = (f"encontrado en {result['info']['fuente']}" if result["info"]["fuente"]
                          else "sin coincidencia externa")
            print(f"    [✓] Metadatos: {file_name} ({encontrado})")

        else:
            raise ValueError(f"Operación desconocida: {operation}")

        # Enviar el resultado al coordinador
        result["result_path"] = upload_result(subtask_id, out_file)
        Path(out_file).unlink(missing_ok=True)

    except Cancelled:
        result["status"] = "cancelled"
        print(f"    [⊘] Cancelada: {file_name} (el caso fue cancelado)")
    except Paused:
        # Se devuelve sin procesar; el coordinador la re-encola al reanudar
        result["status"] = "paused"
        print(f"    [||] En pausa: {file_name} (se procesará cuando se reanude el caso)")
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


def machine_info() -> dict:
    """
    Datos de la computadora donde corre el worker, para demostrar en el
    dashboard que cada worker está en una máquina distinta. Dentro de Docker
    el procesador, los núcleos y la RAM que se ven son los de la máquina real.
    """
    en_docker = os.path.exists("/.dockerenv")
    cpu = platform.processor() or ""
    try:
        with open("/proc/cpuinfo") as f:                      # Linux
            cpu = next((l.split(":", 1)[1].strip() for l in f if l.startswith("model name")), cpu)
    except OSError:
        try:                                                  # Windows
            import winreg
            k = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"HARDWARE\DESCRIPTION\System\CentralProcessor\0")
            cpu = winreg.QueryValueEx(k, "ProcessorNameString")[0].strip()
        except Exception:
            pass
    so = f"{platform.system()} {platform.release()}"
    if not en_docker:
        try:
            with open("/etc/os-release") as f:
                so = next((l.split("=", 1)[1].strip().strip('"') for l in f if l.startswith("PRETTY_NAME=")), so)
        except OSError:
            if platform.system() == "Windows":
                so = f"Windows {platform.release()}"
    return {
        # En Docker el nombre del equipo es el del contenedor, salvo que se pase -e HOST_NAME=$(hostname)
        "hostname": os.getenv("HOST_NAME") or socket.gethostname(),
        "cpu": re.sub(r"\s+", " ", cpu)[:80] or "desconocido",
        "nucleos": psutil.cpu_count(logical=True),
        "ram_gb": round(psutil.virtual_memory().total / 2 ** 30, 1),
        "so": so + (" (Docker)" if en_docker else ""),
        "docker": en_docker,
    }


MACHINE = machine_info()


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
                "machine": MACHINE,
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
    print(f"Máquina: {MACHINE['hostname']} · {MACHINE['cpu']} · {MACHINE['nucleos']} núcleos · "
          f"{MACHINE['ram_gb']} GB · {MACHINE['so']}")
    print("=" * 50)

    threading.Thread(target=heartbeat_loop, daemon=True).start()
    start_consuming()
