"""
=== GENERADOR DE DATASET DE PRUEBA ===
Genera con FFmpeg (sin necesitar archivos base) un dataset de ~500 archivos
multimedia organizado en casos homogéneos y heterogéneos, con metadatos.

  - Audio: wav, flac, ogg, mp3 (tonos, ruido, acordes) de 3 s a 90 s
  - Video: mp4, mkv, avi, mov de 320x240 a 1280x720 y de 4 s a 45 s
  - Imágenes: jpg, png de distintas resoluciones
  - Algunos archivos no soportados (.txt, .pdf) y corruptos, para ver fallos

Tamaños: liviano / mediano / pesado (ver TAMANOS). Cada caso trae un
metadata.json (evento, sesión, usuario, lote y metadatos por archivo), y
en la raíz queda catalogo.json con todos los archivos.

Uso:
  python scripts/generar_dataset.py                    # ~500 archivos en ./dataset_prueba
  python scripts/generar_dataset.py --escala 0.2       # versión chica para probar (~100)
  python scripts/generar_dataset.py --salida ./otro --limpiar
  python scripts/generar_dataset.py --base ./mis_archivos   # además mezcla archivos reales

Requisitos: FFmpeg instalado.
"""

import argparse
import json
import random
import shutil
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

random.seed(2026)  # dataset reproducible

EVENTOS  = ["Festival Cultural TEC", "Graduación 2026", "Hackathon San Carlos",
            "Congreso de Sistemas Operativos", "Feria Vocacional"]
USUARIOS = ["ana", "luis", "sharon", "marco", "valeria"]
ARTISTAS = ["Coro TEC", "Banda Institucional", "Ensamble Jazz", "DJ Campus", "Trío Latino"]

# Perfiles de tamaño: (duración en s, resolución de video, peso)
TAMANOS = {
    "liviano": {"dur": (3, 8),   "res": "320x240",  "peso": 0.5},
    "mediano": {"dur": (12, 25), "res": "640x360",  "peso": 0.35},
    "pesado":  {"dur": (35, 90), "res": "1280x720", "peso": 0.15},
}

VIDEO_SRC = ["testsrc2", "mandelbrot", "smptebars", "rgbtestsrc", "life"]


def elegir_tamano():
    r, acc = random.random(), 0
    for nombre, t in TAMANOS.items():
        acc += t["peso"]
        if r <= acc:
            return nombre
    return "liviano"


def ffmpeg(args):
    subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", *args],
                   check=True, capture_output=True)


# ------------------------------------------------------------
# Generadores de un archivo (devuelven metadatos técnicos)
# ------------------------------------------------------------
def gen_audio(path: Path, tamano: str, meta: dict):
    d = TAMANOS[tamano]
    dur = random.randint(*d["dur"])
    f1, f2 = random.choice([220, 262, 330, 392, 440, 523]), random.choice([0.5, 1, 2])
    fuente = random.choice([
        f"sine=frequency={f1}:duration={dur}",
        f"aevalsrc=0.4*sin({f1}*2*PI*t)*sin({f2}*2*PI*t):d={dur}:s=44100",
        f"anoisesrc=d={dur}:c=pink:a=0.2",
    ])
    args = ["-f", "lavfi", "-i", fuente, "-ac", "2"]
    if path.suffix == ".mp3":
        args += ["-metadata", f"title={meta['titulo']}", "-metadata", f"artist={meta['artista']}",
                 "-metadata", f"album={meta['album']}", "-b:a", "128k"]
    ffmpeg(args + [str(path)])
    return {"duracion_s": dur}


def gen_video(path: Path, tamano: str, meta: dict):
    d = TAMANOS[tamano]
    dur = random.randint(*d["dur"])
    dur = min(dur, 45)
    src = random.choice(VIDEO_SRC)
    vsrc = (f"{src}=size={d['res']}:rate=25" if src != "life"
            else f"life=size={d['res']}:rate=25:mold=10:ratio=0.1")
    args = ["-f", "lavfi", "-i", vsrc, "-f", "lavfi", "-i", f"sine=frequency={random.choice([220, 440, 660])}",
            "-t", str(dur), "-shortest", "-pix_fmt", "yuv420p"]
    # Bien comprimidos para que el dataset no pese tanto; procesarlos sigue
    # siendo costoso porque el costo depende de la resolución y la duración.
    if path.suffix in (".mp4", ".mkv", ".mov"):
        args += ["-c:v", "libx264", "-preset", "veryfast", "-crf", "28", "-b:a", "96k"]
    else:
        args += ["-q:v", "6"]
    ffmpeg(args + [str(path)])
    return {"duracion_s": dur, "resolucion": d["res"]}


def gen_imagen(path: Path, tamano: str, meta: dict):
    res = {"liviano": "640x480", "mediano": "1920x1080", "pesado": "2560x1440"}[tamano]
    src = random.choice(["testsrc2", "mandelbrot", "smptehdbars", "rgbtestsrc"])
    ffmpeg(["-f", "lavfi", "-i", f"{src}=size={res}", "-frames:v", "1", str(path)])
    return {"resolucion": res}


def gen_no_soportado(path: Path, tamano: str, meta: dict):
    if path.suffix == ".mp4":                     # video corrupto (bytes al azar)
        path.write_bytes(random.randbytes(30_000))
    else:
        path.write_text(f"Archivo de notas de {meta['evento']} — formato no soportado\n" * 20)
    return {}


GEN = {"audio": gen_audio, "video": gen_video, "imagen": gen_imagen, "otro": gen_no_soportado}
EXT = {
    "audio_conv": [".wav", ".flac", ".ogg"],
    "audio_mp3":  [".mp3"],
    "video":      [".mp4", ".mkv", ".avi", ".mov"],
    "imagen":     [".jpg", ".png"],
    "otro":       [".txt", ".pdf"],
}


# ------------------------------------------------------------
# Plan del dataset: lista de casos, cada uno con sus archivos
# ------------------------------------------------------------
def planificar(escala: float):
    casos = []
    n = lambda x: max(1, round(x * escala))

    def caso(prefijo, tipo, composicion, descripcion):
        evento = random.choice(EVENTOS)
        sesion = f"S{random.randint(1, 4)}"
        usuario = random.choice(USUARIOS)
        lote = f"L-{len(casos) + 1:03d}"
        nombre = f"caso_{len(casos) + 1:03d}_{prefijo}"
        archivos = []
        for clase, cantidad in composicion:
            for _ in range(cantidad):
                ext = random.choice(EXT[clase])
                gen = ("audio" if clase.startswith("audio") else clase)
                archivos.append((gen, ext))
        casos.append({"nombre": nombre, "tipo": tipo, "evento": evento, "sesion": sesion,
                      "usuario": usuario, "lote": lote, "descripcion": descripcion,
                      "prioridad": random.choice([3, 5, 5, 5, 7, 10]), "archivos": archivos})

    for _ in range(n(7)):
        caso("audio_conversion", "homogeneo", [("audio_conv", 15)], "Lote de grabaciones a convertir a MP3")
    for _ in range(n(4)):
        caso("audio_metadatos", "homogeneo", [("audio_mp3", 15)], "Canciones MP3 para catalogar metadatos")
    for _ in range(n(6)):
        caso("video", "homogeneo", [("video", 6)], "Videos de una sesión para transcodificar")
    for _ in range(n(4)):
        caso("imagenes", "homogeneo", [("imagen", 20)], "Fotografías para generar miniaturas")
    for i in range(n(13)):
        comp = [("audio_conv", 4), ("audio_mp3", 2), ("video", 3), ("imagen", 6)]
        if i % 3 == 0:
            comp.append(("otro", 1))            # algunos casos con un archivo no soportado/corrupto
        caso("heterogeneo", "heterogeneo", comp, "Material completo de un evento (audio, video e imágenes)")
    return casos


def main():
    ap = argparse.ArgumentParser(description="Genera el dataset multimedia de prueba")
    ap.add_argument("--salida", default="./dataset_prueba")
    ap.add_argument("--escala", type=float, default=1.0, help="1.0 ≈ 500 archivos")
    ap.add_argument("--limpiar", action="store_true", help="borrar la carpeta de salida si existe")
    ap.add_argument("--base", help="carpeta con archivos reales para mezclar en los casos heterogéneos")
    ap.add_argument("--hilos", type=int, default=4, help="procesos FFmpeg en paralelo")
    args = ap.parse_args()

    if not shutil.which("ffmpeg"):
        raise SystemExit("[!] FFmpeg no está instalado")

    salida = Path(args.salida)
    if salida.exists():
        if not args.limpiar:
            raise SystemExit(f"[!] {salida} ya existe. Usá --limpiar para regenerarlo.")
        shutil.rmtree(salida)
    salida.mkdir(parents=True)

    casos = planificar(args.escala)
    reales = []
    if args.base:
        reales = [p for p in Path(args.base).iterdir() if p.is_file()]

    trabajos, catalogo = [], []
    for c in casos:
        cdir = salida / c["nombre"]
        cdir.mkdir()
        c["files"] = {}
        for i, (gen, ext) in enumerate(c["archivos"], 1):
            tamano = elegir_tamano() if gen != "otro" else "liviano"
            # Nombres únicos en todo el dataset (c007_video_03_pesado.mkv), para que
            # al reagrupar por metadatos no choquen archivos de distintos casos
            pref = f"c{c['nombre'][5:8]}"
            nombre = (f"{pref}_corrupto_{i:02d}{ext}" if gen == "otro" and ext == ".mp4" else
                      f"{pref}_notas_{i:02d}{ext}" if gen == "otro" else
                      f"{pref}_{gen}_{i:02d}_{tamano}{ext}")
            meta = {
                "titulo": f"{c['evento']} — {gen} {i}",
                "artista": random.choice(ARTISTAS) if gen == "audio" else None,
                "album": c["evento"] if gen == "audio" else None,
                "evento": c["evento"], "sesion": c["sesion"], "usuario": c["usuario"],
                "lote": c["lote"], "tamano": tamano, "tipo": gen,
            }
            c["files"][nombre] = meta
            trabajos.append((gen, cdir / nombre, tamano, meta))
        if reales and c["tipo"] == "heterogeneo":
            for r in random.sample(reales, min(2, len(reales))):
                shutil.copy2(r, cdir / r.name)
                c["files"][r.name] = {"titulo": r.stem, "evento": c["evento"], "sesion": c["sesion"],
                                      "usuario": c["usuario"], "lote": c["lote"], "tipo": "real"}

    print(f"[*] Generando {len(trabajos)} archivos en {len(casos)} casos con {args.hilos} hilos...")
    t0, hechos, errores = time.time(), 0, 0
    with ThreadPoolExecutor(max_workers=args.hilos) as pool:
        futuros = {pool.submit(GEN[g], p, t, m): (p, m) for g, p, t, m in trabajos}
        for fut in as_completed(futuros):
            p, m = futuros[fut]
            try:
                m.update({k: v for k, v in fut.result().items()})
            except Exception as e:
                errores += 1
                print(f"  [!] {p.name}: {e}")
            hechos += 1
            if hechos % 50 == 0:
                print(f"  {hechos}/{len(trabajos)} ({time.time() - t0:.0f} s)")

    # metadata.json por caso + catálogo global
    total_bytes = 0
    resumen = {"homogeneo": 0, "heterogeneo": 0}
    por_tipo, por_tamano = {}, {}
    for c in casos:
        cdir = salida / c["nombre"]
        for nombre, m in c["files"].items():
            f = cdir / nombre
            m["bytes"] = f.stat().st_size if f.exists() else 0
            total_bytes += m["bytes"]
            por_tipo[m["tipo"]] = por_tipo.get(m["tipo"], 0) + 1
            if m.get("tamano"):
                por_tamano[m["tamano"]] = por_tamano.get(m["tamano"], 0) + 1
            catalogo.append({"caso": c["nombre"], "archivo": nombre, **m})
        resumen[c["tipo"]] += 1
        info = {k: c[k] for k in ("tipo", "evento", "sesion", "usuario", "lote", "descripcion", "prioridad")}
        (cdir / "metadata.json").write_text(json.dumps(
            {"case_name": c["nombre"], **info, "file_count": len(c["files"]), "files": c["files"]},
            ensure_ascii=False, indent=2), encoding="utf-8")

    (salida / "catalogo.json").write_text(json.dumps(catalogo, ensure_ascii=False, indent=2), encoding="utf-8")
    composicion = {
        "archivos": len(catalogo), "casos": len(casos), "casos_por_tipo": resumen,
        "archivos_por_tipo": por_tipo, "archivos_por_tamano": por_tamano,
        "volumen_total_mb": round(total_bytes / 1024 / 1024, 1),
    }
    (salida / "composicion.json").write_text(json.dumps(composicion, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n" + "=" * 50)
    print(f"Dataset listo en {salida.resolve()}  ({time.time() - t0:.0f} s, {errores} errores)")
    print(json.dumps(composicion, ensure_ascii=False, indent=2))
    print("=" * 50)


if __name__ == "__main__":
    main()
