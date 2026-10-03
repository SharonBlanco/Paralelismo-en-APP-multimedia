"""
=== DATASET CON ARCHIVOS REALES (Wikimedia Commons) ===
Descarga videos, audios e imágenes reales con licencia libre desde
Wikimedia Commons (conciertos, festivales, graduaciones, conferencias...)
y arma con ellos un dataset de ~480 archivos organizado en casos
homogéneos y heterogéneos, igual que scripts/generar_dataset.py.

Para lograr variedad de formatos y tamaños sin descargar cientos de
archivos, a partir de cada original se recortan fragmentos de distinta
duración y resolución y se exportan a mp4, mkv, avi, mov, wav, flac, ogg,
mp3, jpg y png.

Cada archivo conserva sus metadatos reales (título, autor, licencia y
enlace) en metadata.json / catalogo.json, y se genera CREDITOS.md con la
atribución que piden las licencias Creative Commons.

Uso:
  python scripts/descargar_dataset.py                       # ./dataset_real
  python scripts/descargar_dataset.py --escala 0.2          # versión chica
  python scripts/descargar_dataset.py --videos 8 --audios 15 --imagenes 20

Requisitos: FFmpeg instalado y conexión a internet.
Los originales quedan en <salida>/_originales y se reutilizan si se vuelve
a correr el script (no se descargan de nuevo).
"""

import argparse
import hashlib
import json
import random
import re
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from html import unescape
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).parent))
from generar_dataset import planificar, elegir_tamano, EVENTOS, USUARIOS  # mismo plan de casos

random.seed(2026)

API = "https://commons.wikimedia.org/w/api.php"
# Wikimedia pide un User-Agent que identifique al cliente
HEADERS = {"User-Agent": "ProyectoSO-TEC-dataset/1.0 (https://github.com/SharonBlanco/Proyecto; uso académico)"}

BUSQUEDAS = {
    "video":  ["concert", "music festival", "dance performance", "orchestra", "parade", "street music",
               "university lecture", "choir"],
    "audio":  ["piano", "guitar", "orchestra", "choir", "folk music", "jazz", "speech", "violin"],
    "imagen": ["concert stage", "music festival", "graduation ceremony", "university campus",
               "conference audience", "orchestra", "street parade", "theatre performance"],
}
FILTRO = {   # tipo de Commons y rango de tamaño en KB
    "video":  "filetype:video filesize:300,25000",
    "audio":  "filetype:audio filesize:100,10000",
    "imagen": "filetype:bitmap filesize:800,20000",
}
MIMES_OK = {
    "video":  ("video/", "application/ogg"),
    "audio":  ("audio/", "application/ogg"),
    "imagen": ("image/jpeg", "image/png"),
}

# Cómo se derivan los archivos según su tamaño (realista: una canción dura
# ~2.5 min, un video "pesado" son 3 min en Full HD, una foto pesada es 4K)
PERFIL = {
    "video":  {"liviano": (20, 360),   "mediano": (75, 720),    "pesado": (180, 1080)},   # (segundos, alto px)
    "audio":  {"liviano": (30, None),  "mediano": (150, None),  "pesado": (420, None)},
    "imagen": {"liviano": (None, 1280), "mediano": (None, 2560), "pesado": (None, 4096)},  # ancho px
}
# Perfil liviano (--ligero) para probar rápido
PERFIL_LIGERO = {
    "video":  {"liviano": (6, 240),   "mediano": (15, 480),  "pesado": (40, 720)},
    "audio":  {"liviano": (8, None),  "mediano": (30, None), "pesado": (90, None)},
    "imagen": {"liviano": (None, 800), "mediano": (None, 1600), "pesado": (None, 2560)},
}


# ============================================================
# 1. BÚSQUEDA Y DESCARGA DE ORIGINALES
# ============================================================
def limpiar_html(texto):
    return re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", "", texto or ""))).strip()


def buscar(tipo, termino, limite):
    params = {
        "action": "query", "format": "json", "generator": "search", "gsrnamespace": 6,
        "gsrlimit": limite, "gsrsearch": f"{termino} {FILTRO[tipo]}",
        "prop": "imageinfo", "iiprop": "url|size|mime|extmetadata",
        "iiextmetadatafilter": "Artist|LicenseShortName|ObjectName|ImageDescription",
    }
    r = requests.get(API, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    resultados = []
    for p in r.json().get("query", {}).get("pages", {}).values():
        ii = (p.get("imageinfo") or [{}])[0]
        mime = ii.get("mime", "")
        if not mime.startswith(MIMES_OK[tipo]):
            continue
        m = ii.get("extmetadata", {})
        titulo = limpiar_html(m.get("ObjectName", {}).get("value")) or Path(p["title"][5:]).stem
        resultados.append({
            "tipo": tipo,
            "commons": p["title"],
            "titulo": titulo[:120],
            "autor": limpiar_html(m.get("Artist", {}).get("value"))[:120] or "Desconocido",
            "licencia": limpiar_html(m.get("LicenseShortName", {}).get("value")) or "ver fuente",
            "fuente": ii.get("descriptionurl"),
            "url": ii.get("url"),
            "mime": mime,
        })
    return resultados


def recolectar(tipo, cantidad):
    """Busca candidatos variados (varios términos) hasta juntar `cantidad`"""
    vistos, elegidos = set(), []
    terminos = BUSQUEDAS[tipo][:]
    random.shuffle(terminos)
    por_termino = max(3, cantidad // len(terminos) + 2)
    for termino in terminos:
        try:
            for c in buscar(tipo, termino, por_termino * 2):
                if c["commons"] not in vistos:
                    vistos.add(c["commons"])
                    c["tema"] = termino
                    elegidos.append(c)
                    if sum(1 for e in elegidos if e["tema"] == termino) >= por_termino:
                        break
        except requests.RequestException as e:
            print(f"  [!] Búsqueda '{termino}' falló: {e}")
        time.sleep(0.3)
    random.shuffle(elegidos)
    return elegidos[:cantidad]


def descargar(c, carpeta: Path):
    ext = Path(c["url"].split("?")[0]).suffix.lower() or ".bin"
    destino = carpeta / f"{hashlib.sha1(c['commons'].encode()).hexdigest()[:12]}{ext}"
    if not destino.exists() or destino.stat().st_size == 0:
        with requests.get(c["url"], headers=HEADERS, stream=True, timeout=120) as r:
            r.raise_for_status()
            tmp = destino.with_suffix(destino.suffix + ".part")
            with open(tmp, "wb") as fp:
                for chunk in r.iter_content(1024 * 256):
                    fp.write(chunk)
            tmp.rename(destino)
    c["archivo"] = str(destino)
    return c


def sondear(c):
    """Duración y si tiene pista de audio (ffprobe)"""
    try:
        out = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration:stream=codec_type",
             "-of", "json", c["archivo"]], capture_output=True, text=True, timeout=60).stdout
        info = json.loads(out or "{}")
        c["duracion"] = float(info.get("format", {}).get("duration") or 0)
        tipos = {s.get("codec_type") for s in info.get("streams", [])}
        c["tiene_audio"] = "audio" in tipos
        c["tiene_video"] = "video" in tipos
        c["ok"] = (c["tipo"] == "imagen" or c["duracion"] > 1) and (
            c["tipo"] != "video" or c["tiene_video"]) and (c["tipo"] != "audio" or c["tiene_audio"])
    except (subprocess.SubprocessError, ValueError):
        c["ok"] = False
    return c


# ============================================================
# 2. DERIVAR ARCHIVOS (recortes, resoluciones y formatos)
# ============================================================
def ffmpeg(args, timeout=1800):
    subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", *args],
                   check=True, capture_output=True, timeout=timeout)


def entrada(o, dur):
    """Fragmento al azar del original; si es más corto que dur, se repite en bucle"""
    if o["duracion"] > dur:
        inicio = random.uniform(0, o["duracion"] - dur)
        return ["-ss", f"{inicio:.2f}", "-t", str(dur), "-i", o["archivo"]]
    return ["-stream_loop", "-1", "-t", str(dur), "-i", o["archivo"]]


def derivar_video(o, destino: Path, tamano):
    dur, alto = PERFIL["video"][tamano]
    args = entrada(o, dur)
    if not o["tiene_audio"]:   # garantizar pista de audio (para extraer audio después)
        args += ["-f", "lavfi", "-t", str(dur), "-i", "anullsrc=r=44100:cl=stereo"]
    args += ["-map", "0:v:0", "-map", "0:a:0" if o["tiene_audio"] else "1:a:0",
             "-vf", f"scale=-2:{alto}", "-pix_fmt", "yuv420p", "-shortest"]
    if destino.suffix == ".avi":
        args += ["-c:v", "mpeg4", "-q:v", "4", "-c:a", "libmp3lame", "-b:a", "192k"]
    else:
        args += ["-c:v", "libx264", "-preset", "veryfast", "-crf", "22", "-c:a", "aac", "-b:a", "160k"]
    ffmpeg(args + [str(destino)])
    return {"duracion_s": dur, "alto_px": alto}


def derivar_audio(o, destino: Path, tamano, meta):
    dur, _ = PERFIL["audio"][tamano]
    args = entrada(o, dur) + ["-vn", "-ac", "2", "-ar", "44100"]
    if destino.suffix == ".mp3":
        args += ["-c:a", "libmp3lame", "-b:a", "192k", "-metadata", f"title={meta['titulo']}",
                 "-metadata", f"artist={meta['artista']}", "-metadata", f"album={meta['album']}",
                 "-metadata", f"comment={o['licencia']} - {o['fuente']}"]
    ffmpeg(args + [str(destino)])
    return {"duracion_s": dur}


def derivar_imagen(o, destino: Path, tamano):
    _, ancho = PERFIL["imagen"][tamano]
    args = ["-i", o["archivo"], "-vf", f"scale={ancho}:-2", "-frames:v", "1"]
    if destino.suffix == ".jpg":
        args += ["-q:v", "3"]
    ffmpeg(args + [str(destino)])
    return {"ancho_px": ancho}


def derivar_muy_pesado(o, destino: Path, objetivo_mb: float):
    """
    Archivo de 400–600 MB (lo que pidió el profesor para probar cargas
    reales). Para no codificar una hora de video, se codifica 1 minuto en
    Full HD con alta calidad y luego se repite sin recodificar (-c copy)
    hasta alcanzar el tamaño. Para audio se genera un WAV sin comprimir.
    """
    objetivo = objetivo_mb * 2 ** 20
    if o["tipo"] == "audio":
        dur = objetivo / (44100 * 2 * 2)                      # PCM 16 bits estéreo
        ffmpeg(["-stream_loop", "-1", "-t", f"{dur:.0f}", "-i", o["archivo"], "-vn",
                "-ac", "2", "-ar", "44100", "-c:a", "pcm_s16le", str(destino)])
        return {"duracion_s": round(dur)}

    base = destino.with_name(destino.stem + ".base.mp4")
    args = entrada(o, 60)
    if not o["tiene_audio"]:
        args += ["-f", "lavfi", "-t", "60", "-i", "anullsrc=r=44100:cl=stereo"]
    args += ["-map", "0:v:0", "-map", "0:a:0" if o["tiene_audio"] else "1:a:0",
             "-vf", "scale=-2:1080", "-pix_fmt", "yuv420p", "-shortest",
             "-c:v", "libx264", "-preset", "veryfast", "-crf", "17", "-c:a", "aac", "-b:a", "192k"]
    ffmpeg(args + [str(base)])
    bytes_por_s = base.stat().st_size / 60
    dur = objetivo / bytes_por_s
    ffmpeg(["-stream_loop", "-1", "-t", f"{dur:.0f}", "-i", str(base), "-c", "copy", str(destino)])
    base.unlink(missing_ok=True)
    return {"duracion_s": round(dur), "alto_px": 1080}


def meta_de(o, c, tamano, tipo):
    return {"titulo": o["titulo"], "artista": o["autor"], "album": c["evento"],
            "licencia": o["licencia"], "fuente": o["fuente"], "original": o["commons"],
            "evento": c["evento"], "sesion": c["sesion"], "usuario": c["usuario"],
            "lote": c["lote"], "tamano": tamano, "tipo": tipo}


def plan_pesados(desde, cantidad, n_muy, originales, salida):
    """
    Casos heterogéneos con SOLO archivos pesados: en cada uno, 1 video de
    3 min en Full HD, 3 audios de 7 min y 5 fotos 4K; y en los primeros
    `n_muy` casos, además, un archivo de 400–600 MB (2/3 videos, 1/3 WAV).
    """
    n_video = max(1, round(n_muy * 2 / 3)) if n_muy else 0
    muy = ["video"] * n_video + ["audio"] * (n_muy - n_video)
    casos, trabajos = [], []
    for j in range(cantidad):
        num = desde + j
        c = {"nombre": f"caso_{num:03d}_heterogeneo", "tipo": "heterogeneo",
             "evento": random.choice(EVENTOS), "sesion": f"S{random.randint(1, 4)}",
             "usuario": random.choice(USUARIOS), "lote": f"L-{num:03d}",
             "descripcion": "Material pesado de un evento: video Full HD, audios largos y fotos 4K",
             "prioridad": random.choice([3, 5, 5, 7, 10]), "files": {}}
        cdir = salida / c["nombre"]
        cdir.mkdir()
        pref = f"c{num:03d}"
        composicion = ([("video", random.choice([".mp4", ".mkv", ".mov", ".avi"]))] +
                       [("audio", random.choice([".wav", ".flac", ".ogg", ".mp3"])) for _ in range(3)] +
                       [("imagen", random.choice([".png", ".jpg"])) for _ in range(5)])
        for i, (gen, ext) in enumerate(composicion, 1):
            o = random.choice(originales[gen])
            nombre = f"{pref}_{gen}_{i:02d}_pesado{ext}"
            c["files"][nombre] = meta_de(o, c, "pesado", gen)
            trabajos.append((gen, o, cdir / nombre, "pesado", c["files"][nombre]))
        if j < len(muy):
            tipo = muy[j]
            ext = random.choice([".mp4", ".mkv", ".mov"]) if tipo == "video" else ".wav"
            o = random.choice(originales[tipo])
            nombre = f"{pref}_{tipo}_{len(composicion) + 1:02d}_muy_pesado{ext}"
            c["files"][nombre] = meta_de(o, c, "muy_pesado", tipo)
            trabajos.append(("muy_pesado", o, cdir / nombre, random.uniform(430, 570), c["files"][nombre]))
        casos.append(c)
    return casos, trabajos


def numero_caso(d: Path):
    try:
        return int(d.name.split("_")[1])
    except (IndexError, ValueError):
        return None


# ============================================================
# MAIN
# ============================================================
def main():
    ap = argparse.ArgumentParser(description="Dataset con archivos reales de Wikimedia Commons")
    ap.add_argument("--salida", default="./dataset_real")
    ap.add_argument("--escala", type=float, default=1.0, help="1.0 ≈ 480 archivos derivados")
    ap.add_argument("--videos", type=int, default=12, help="originales de video a descargar")
    ap.add_argument("--audios", type=int, default=20, help="originales de audio a descargar")
    ap.add_argument("--imagenes", type=int, default=30, help="originales de imagen a descargar")
    ap.add_argument("--limpiar", action="store_true", help="borrar los casos generados antes (conserva _originales)")
    ap.add_argument("--hilos", type=int, default=4)
    ap.add_argument("--muy-pesados", type=int, default=10,
                    help="archivos de 400–600 MB (2/3 videos largos, 1/3 WAV); 0 para no generarlos")
    ap.add_argument("--ligero", action="store_true", help="tamaños chicos, para probar rápido")
    ap.add_argument("--pesados-desde", type=int, metavar="N",
                    help="conserva los casos anteriores a N y reemplaza desde el N en adelante por "
                         "casos heterogéneos con solo archivos pesados")
    ap.add_argument("--casos-pesados", type=int, default=14, help="cuántos casos pesados crear (con --pesados-desde)")
    args = ap.parse_args()
    if args.ligero:
        PERFIL.update(PERFIL_LIGERO)

    if not shutil.which("ffmpeg"):
        raise SystemExit("[!] FFmpeg no está instalado")

    salida = Path(args.salida)
    if args.pesados_desde:
        # Solo se borran los casos desde N en adelante; los anteriores quedan intactos
        for d in list(salida.glob("caso_*")) if salida.exists() else []:
            if (numero_caso(d) or 0) >= args.pesados_desde:
                shutil.rmtree(d)
    elif salida.exists() and any(d.name.startswith("caso_") for d in salida.iterdir()):
        if not args.limpiar:
            raise SystemExit(f"[!] {salida} ya tiene casos. Usá --limpiar para regenerarlos.")
        for d in salida.iterdir():
            if d.name != "_originales":
                shutil.rmtree(d) if d.is_dir() else d.unlink()
    orig_dir = salida / "_originales"
    orig_dir.mkdir(parents=True, exist_ok=True)
    t0 = time.time()

    # --- 1. buscar y descargar originales ---
    originales = {}
    for tipo, n in (("video", args.videos), ("audio", args.audios), ("imagen", args.imagenes)):
        print(f"[*] Buscando {n} {tipo}s en Wikimedia Commons...")
        candidatos = recolectar(tipo, n)
        carpeta = orig_dir / tipo
        carpeta.mkdir(exist_ok=True)
        listos = []
        with ThreadPoolExecutor(max_workers=3) as pool:        # pocas descargas a la vez: ser amables
            futuros = {pool.submit(descargar, c, carpeta): c for c in candidatos}
            for fut in as_completed(futuros):
                c = futuros[fut]
                try:
                    c = sondear(fut.result())
                    if c["ok"]:
                        listos.append(c)
                    else:
                        print(f"  [!] Descartado (no se pudo leer): {c['commons']}")
                except Exception as e:
                    print(f"  [!] No se pudo descargar {c['commons']}: {e}")
        originales[tipo] = listos
        mb = sum(Path(c["archivo"]).stat().st_size for c in listos) / 2 ** 20
        print(f"  [✓] {len(listos)} {tipo}s ({mb:.0f} MB)")
        if not listos:
            raise SystemExit(f"[!] No se consiguió ningún {tipo}. Revisá la conexión e intentá de nuevo.")

    if args.pesados_desde:
        # Casos ya existentes (se conservan tal cual) + casos pesados nuevos
        casos = []
        for d in sorted(salida.glob("caso_*"), key=lambda d: numero_caso(d) or 0):
            meta_path = d / "metadata.json"
            if (numero_caso(d) or 0) < args.pesados_desde and meta_path.exists():
                m = json.loads(meta_path.read_text(encoding="utf-8"))
                casos.append({"nombre": d.name, "tipo": m.get("tipo", "homogeneo"), "files": m.get("files", {}),
                              **{k: m.get(k) for k in ("evento", "sesion", "usuario", "lote",
                                                       "descripcion", "prioridad")}})
        print(f"[*] Se conservan {len(casos)} casos (del 1 al {args.pesados_desde - 1})")
        nuevos, trabajos = plan_pesados(args.pesados_desde, args.casos_pesados, args.muy_pesados,
                                        originales, salida)
        casos += nuevos
    else:
        # --- 2. plan de casos y archivos derivados ---
        casos = planificar(args.escala)
        trabajos = []
        for c in casos:
            cdir = salida / c["nombre"]
            cdir.mkdir()
            c["files"] = {}
            pref = f"c{c['nombre'][5:8]}"
            for i, (gen, ext) in enumerate(c["archivos"], 1):
                if gen == "otro":
                    nombre = f"{pref}_corrupto_{i:02d}{ext}" if ext == ".mp4" else f"{pref}_notas_{i:02d}{ext}"
                    c["files"][nombre] = {"titulo": "Archivo no soportado", "evento": c["evento"],
                                          "sesion": c["sesion"], "usuario": c["usuario"], "lote": c["lote"],
                                          "tipo": "otro", "tamano": "liviano"}
                    trabajos.append(("otro", None, cdir / nombre, "liviano", c["files"][nombre]))
                    continue
                tamano = elegir_tamano()
                o = random.choice(originales[gen])
                nombre = f"{pref}_{gen}_{i:02d}_{tamano}{ext}"
                meta = {
                    "titulo": o["titulo"], "artista": o["autor"], "album": c["evento"],
                    "licencia": o["licencia"], "fuente": o["fuente"], "original": o["commons"],
                    "evento": c["evento"], "sesion": c["sesion"], "usuario": c["usuario"],
                    "lote": c["lote"], "tamano": tamano, "tipo": gen,
                }
                c["files"][nombre] = meta
                trabajos.append((gen, o, cdir / nombre, tamano, meta))

        # --- archivos muy pesados (400–600 MB) ---
        # El primero va en un caso que replica el ejemplo de la clase: un lote
        # chico (4 mp3) con un archivo enorme; el resto, uno por caso heterogéneo.
        if args.muy_pesados > 0:
            n_video = max(1, round(args.muy_pesados * 2 / 3))
            tipos = ["video"] * n_video + ["audio"] * (args.muy_pesados - n_video)
            lote = {"nombre": f"caso_{len(casos) + 1:03d}_lote_con_archivo_pesado", "tipo": "heterogeneo",
                    "evento": "Graduación 2026", "sesion": "S1", "usuario": "sharon", "lote": f"L-{len(casos) + 1:03d}",
                    "descripcion": "Lote chico (4 canciones) con un video de cientos de MB", "prioridad": 5,
                    "archivos": [("audio", ".mp3")] * 4}
            casos.append(lote)
            cdir = salida / lote["nombre"]
            cdir.mkdir()
            lote["files"] = {}
            pref = f"c{lote['nombre'][5:8]}"
            for i in range(4):
                o = random.choice(originales["audio"])
                nombre = f"{pref}_audio_{i + 1:02d}_mediano.mp3"
                lote["files"][nombre] = {"titulo": o["titulo"], "artista": o["autor"], "album": lote["evento"],
                                         "licencia": o["licencia"], "fuente": o["fuente"], "original": o["commons"],
                                         "evento": lote["evento"], "sesion": "S1", "usuario": "sharon",
                                         "lote": lote["lote"], "tamano": "mediano", "tipo": "audio"}
                trabajos.append(("audio", o, cdir / nombre, "mediano", lote["files"][nombre]))

            destinos = [lote] + [c for c in casos if c["tipo"] == "heterogeneo" and c is not lote]
            for k, tipo in enumerate(tipos):
                c = destinos[k % len(destinos)]
                ext = random.choice([".mp4", ".mkv", ".mov"]) if tipo == "video" else ".wav"
                o = random.choice(originales[tipo])
                pref = f"c{c['nombre'][5:8]}"
                nombre = f"{pref}_{tipo}_{len(c['files']) + 1:02d}_muy_pesado{ext}"
                meta = {"titulo": o["titulo"], "artista": o["autor"], "album": c["evento"],
                        "licencia": o["licencia"], "fuente": o["fuente"], "original": o["commons"],
                        "evento": c["evento"], "sesion": c["sesion"], "usuario": c["usuario"],
                        "lote": c["lote"], "tamano": "muy_pesado", "tipo": tipo}
                c["files"][nombre] = meta
                trabajos.append(("muy_pesado", o, salida / c["nombre"] / nombre, random.uniform(430, 570), meta))

    print(f"[*] Generando {len(trabajos)} archivos derivados en {len(casos)} casos...")
    errores, hechos = 0, 0

    def hacer(gen, o, destino, tamano, meta):
        if gen == "muy_pesado":                                # aquí "tamano" es el objetivo en MB
            # Algunos originales no se pueden repetir en bucle: probar con otros
            candidatos = [o] + random.sample(originales[o["tipo"]], min(3, len(originales[o["tipo"]])))
            for intento, orig in enumerate(candidatos):
                try:
                    r = derivar_muy_pesado(orig, destino, tamano)
                    meta.update(titulo=orig["titulo"], artista=orig["autor"], licencia=orig["licencia"],
                                fuente=orig["fuente"], original=orig["commons"])
                    return r
                except subprocess.CalledProcessError:
                    destino.unlink(missing_ok=True)
                    if intento == len(candidatos) - 1:
                        raise
        if gen == "video":
            return derivar_video(o, destino, tamano)
        if gen == "audio":
            return derivar_audio(o, destino, tamano, meta)
        if gen == "imagen":
            return derivar_imagen(o, destino, tamano)
        if destino.suffix == ".mp4":                       # video corrupto
            destino.write_bytes(random.randbytes(30_000))
        else:
            destino.write_text(f"Notas de {meta['evento']} (formato no soportado por la plataforma)\n" * 20,
                               encoding="utf-8")
        return {}

    with ThreadPoolExecutor(max_workers=args.hilos) as pool:
        futuros = {pool.submit(hacer, *t): t for t in trabajos}
        for fut in as_completed(futuros):
            gen, o, destino, tamano, meta = futuros[fut]
            try:
                meta.update(fut.result())
            except Exception as e:
                errores += 1
                err = getattr(e, "stderr", b"") or b""
                print(f"  [!] {destino.name}: {err.decode(errors='ignore')[:150] if isinstance(err, bytes) else e}")
            hechos += 1
            if hechos % 50 == 0:
                print(f"  {hechos}/{len(trabajos)} ({time.time() - t0:.0f} s)")

    # --- 3. metadatos, catálogo, composición y créditos ---
    catalogo, total_bytes = [], 0
    resumen, por_tipo, por_tamano, por_formato = {"homogeneo": 0, "heterogeneo": 0}, {}, {}, {}
    for c in casos:
        cdir = salida / c["nombre"]
        for nombre in list(c["files"]):
            f = cdir / nombre
            if not f.exists():                 # falló la conversión: se quita del caso
                del c["files"][nombre]
                continue
            m = c["files"][nombre]
            m["bytes"] = f.stat().st_size
            total_bytes += m["bytes"]
            por_tipo[m["tipo"]] = por_tipo.get(m["tipo"], 0) + 1
            por_tamano[m["tamano"]] = por_tamano.get(m["tamano"], 0) + 1
            por_formato[f.suffix[1:]] = por_formato.get(f.suffix[1:], 0) + 1
            catalogo.append({"caso": c["nombre"], "archivo": nombre, **m})
        resumen[c["tipo"]] += 1
        info = {k: c[k] for k in ("tipo", "evento", "sesion", "usuario", "lote", "descripcion", "prioridad")}
        (cdir / "metadata.json").write_text(json.dumps(
            {"case_name": c["nombre"], **info, "origen": "Wikimedia Commons (recortado y convertido)",
             "file_count": len(c["files"]), "files": c["files"]}, ensure_ascii=False, indent=2), encoding="utf-8")

    (salida / "catalogo.json").write_text(json.dumps(catalogo, ensure_ascii=False, indent=2), encoding="utf-8")

    usados = {m["original"] for m in catalogo if m.get("original")}
    todos = [o for lista in originales.values() for o in lista]
    composicion = {
        "origen": "Wikimedia Commons",
        "originales_descargados": {t: len(v) for t, v in originales.items()},
        "originales_usados": len(usados),
        "archivos": len(catalogo), "casos": len(casos), "casos_por_tipo": resumen,
        "archivos_por_tipo": por_tipo, "archivos_por_tamano": por_tamano, "archivos_por_formato": por_formato,
        "volumen_total_mb": round(total_bytes / 2 ** 20, 1),
        "errores": errores,
    }
    (salida / "composicion.json").write_text(json.dumps(composicion, ensure_ascii=False, indent=2), encoding="utf-8")

    lineas = ["# Créditos del dataset", "",
              "Archivos originales obtenidos de Wikimedia Commons. Los archivos del dataset son "
              "fragmentos recortados, reescalados y convertidos de formato a partir de ellos.", "",
              "| Tipo | Título | Autor | Licencia | Fuente |", "|---|---|---|---|---|"]
    for o in sorted(todos, key=lambda o: (o["tipo"], o["titulo"])):
        if o["commons"] in usados:
            fila = [o["tipo"], o["titulo"], o["autor"], o["licencia"], f"[enlace]({o['fuente']})"]
            lineas.append("| " + " | ".join(x.replace("|", "/") for x in fila) + " |")
    (salida / "CREDITOS.md").write_text("\n".join(lineas) + "\n", encoding="utf-8")

    print("\n" + "=" * 50)
    print(f"Dataset listo en {salida.resolve()}  ({time.time() - t0:.0f} s)")
    print(json.dumps(composicion, ensure_ascii=False, indent=2))
    print(f"Créditos: {salida / 'CREDITOS.md'}")
    print("=" * 50)


if __name__ == "__main__":
    main()
