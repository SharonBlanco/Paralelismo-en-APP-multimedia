"""
=== CLIENTE / GENERADOR DE CARGA ===
Envía casos al coordinador, consulta su estado, recupera resultados y
genera carga concurrente midiendo tiempos (para el informe de pruebas).

Uso:
  # Un caso manual (todos los archivos de una carpeta; usa su metadata.json si existe)
  python cliente/cliente.py enviar ./dataset_prueba/caso_001_audio_conversion --prioridad 8

  # Carga: cada sub-carpeta del dataset es un caso, 5 envíos concurrentes
  python cliente/cliente.py carga ./dataset_prueba --concurrentes 5 --esperar --metricas pruebas/carga.csv

  # Generación automática: agrupa los archivos del catálogo por metadatos
  python cliente/cliente.py auto ./dataset_prueba --por evento --esperar --metricas pruebas/por_evento.csv

  python cliente/cliente.py estado case-abc12345
  python cliente/cliente.py esperar case-abc12345 case-def67890
  python cliente/cliente.py resultados case-abc12345 --salida ./resultados
  python cliente/cliente.py cancelar case-abc12345
  python cliente/cliente.py resumen

El coordinador se toma de COORDINATOR_URL (por defecto http://localhost:8000).
"""

import os
import csv
import json
import time
import random
import argparse
import requests
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://localhost:8000")
FINAL = {"completed", "partially_completed", "failed", "cancelled"}
IGNORAR = {"metadata.json", "catalogo.json", "composicion.json"}


# ============================================================
# ENVÍO DE CASOS
# ============================================================
def _multipart(campos: dict, archivos, boundary: str):
    """
    Arma el cuerpo multipart/form-data como un flujo: los archivos se leen
    de a 1 MB mientras se envían, así un lote con archivos de cientos de MB
    no se carga entero en memoria.
    """
    for nombre, valor in campos.items():
        yield (f"--{boundary}\r\nContent-Disposition: form-data; name=\"{nombre}\"\r\n\r\n"
               f"{valor}\r\n").encode()
    for f in archivos:
        nombre_archivo = f.name.replace('"', "'")
        yield (f"--{boundary}\r\nContent-Disposition: form-data; name=\"files\"; "
               f"filename=\"{nombre_archivo}\"\r\nContent-Type: application/octet-stream\r\n\r\n").encode()
        with open(f, "rb") as fp:
            while chunk := fp.read(1024 * 1024):
                yield chunk
        yield b"\r\n"
    yield f"--{boundary}--\r\n".encode()


def enviar_archivos(archivos, nombre, prioridad=5, metadata=None):
    """Envía una lista de archivos como un caso. Devuelve la respuesta del coordinador."""
    campos = {"case_name": nombre, "priority": str(prioridad)}
    if metadata:
        campos["metadata"] = json.dumps(metadata, ensure_ascii=False)
    boundary = f"----caso{random.getrandbits(64):016x}"
    total_mb = sum(f.stat().st_size for f in archivos) / 2 ** 20
    try:
        t0 = time.time()
        resp = requests.post(
            f"{COORDINATOR_URL}/api/cases",
            data=_multipart(campos, archivos, boundary),
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
            timeout=(10, 1800))
        resp.raise_for_status()
        result = resp.json()
        result["upload_seconds"] = round(time.time() - t0, 2)
        print(f"[✓] {nombre}: {result['case_id']} | {len(archivos)} archivos ({total_mb:.0f} MB) → "
              f"{result['total_subtasks']} sub-tareas | prioridad {prioridad} | "
              f"subida {result['upload_seconds']} s")
        return result
    except Exception as e:
        print(f"[✗] {nombre}: {e}")
        return None


def leer_metadata(carpeta: Path):
    m = carpeta / "metadata.json"
    return json.loads(m.read_text(encoding="utf-8")) if m.exists() else None


def enviar_caso(carpeta, nombre=None, prioridad=None):
    """Envía todos los archivos de una carpeta como un caso (incluidos los no soportados)"""
    carpeta = Path(carpeta)
    if not carpeta.is_dir():
        print(f"[!] Carpeta no existe: {carpeta}")
        return None
    archivos = sorted(f for f in carpeta.iterdir() if f.is_file() and f.name not in IGNORAR)
    if not archivos:
        print(f"[!] No hay archivos en {carpeta}")
        return None
    meta = leer_metadata(carpeta)
    if prioridad is None:
        prioridad = (meta or {}).get("prioridad", 5)
    return enviar_archivos(archivos, nombre or carpeta.name, prioridad, meta)


# ============================================================
# GENERACIÓN AUTOMÁTICA DE CASOS (agrupación por metadatos)
# ============================================================
def agrupar(dataset: Path, criterio: str):
    """
    Agrupa los archivos del dataset en casos según un criterio:
      carpeta → cada sub-carpeta es un caso
      evento / sesion / usuario / lote → según el catálogo de metadatos
    Devuelve [(nombre_caso, [archivos], metadata)].
    """
    if criterio == "carpeta":
        grupos = []
        for c in sorted(d for d in dataset.iterdir() if d.is_dir()):
            archivos = sorted(f for f in c.iterdir() if f.is_file() and f.name not in IGNORAR)
            if archivos:
                grupos.append((c.name, archivos, leer_metadata(c)))
        return grupos

    catalogo_path = dataset / "catalogo.json"
    if not catalogo_path.exists():
        raise SystemExit(f"[!] Falta {catalogo_path} (lo crea scripts/generar_dataset.py)")
    catalogo = json.loads(catalogo_path.read_text(encoding="utf-8"))

    claves = {"evento": ["evento"], "sesion": ["evento", "sesion"],
              "usuario": ["usuario"], "lote": ["lote"]}[criterio]
    grupos = {}
    for item in catalogo:
        clave = " · ".join(str(item.get(k)) for k in claves)
        grupos.setdefault(clave, []).append(item)

    resultado = []
    for clave, items in sorted(grupos.items()):
        archivos, files_meta = [], {}
        for it in items:
            f = dataset / it["caso"] / it["archivo"]
            if not f.exists():
                continue
            if f.name in files_meta:                         # el coordinador guarda por nombre
                print(f"[!] {f.name} repetido en el grupo '{clave}': se omite")
                continue
            archivos.append(f)
            files_meta[f.name] = {k: v for k, v in it.items() if k not in ("caso", "archivo")}
        meta = {"agrupado_por": criterio, criterio: clave, "files": files_meta}
        resultado.append((f"{criterio}: {clave}", archivos, meta))
    return resultado


# ============================================================
# SEGUIMIENTO
# ============================================================
def esperar(case_ids, intervalo=3, timeout=None):
    """Espera a que todos los casos terminen (estado final). Muestra el avance."""
    pendientes, t0 = set(case_ids), time.time()
    while pendientes:
        try:
            casos = {c["case_id"]: c for c in requests.get(f"{COORDINATOR_URL}/api/cases", timeout=30).json()}
        except requests.RequestException as e:
            print(f"[!] No se pudo consultar el coordinador: {e}")
            time.sleep(intervalo)
            continue
        for cid in list(pendientes):
            c = casos.get(cid)
            if not c or c["status"] in FINAL:
                pendientes.discard(cid)
        hechos = sum(c.get("n_completed", 0) + c.get("n_failed", 0) for cid, c in casos.items() if cid in case_ids)
        total = sum(c.get("total_subtasks", 0) for cid, c in casos.items() if cid in case_ids)
        print(f"\r[*] {len(case_ids) - len(pendientes)}/{len(case_ids)} casos terminados · "
              f"{hechos}/{total} sub-tareas · {time.time() - t0:.0f} s", end="", flush=True)
        if timeout and time.time() - t0 > timeout:
            print("\n[!] Tiempo de espera agotado")
            break
        if pendientes:
            time.sleep(intervalo)
    print()
    return time.time() - t0


def guardar_metricas(case_ids, archivo, inicio_envio, fin_envio, fin_total, concurrentes):
    """Guarda métricas por caso (CSV) y un resumen (JSON) para el informe de pruebas"""
    reportes = []
    for cid in case_ids:
        try:
            reportes.append(requests.get(f"{COORDINATOR_URL}/api/cases/{cid}/report", timeout=30).json())
        except requests.RequestException as e:
            print(f"[!] Sin reporte para {cid}: {e}")

    archivo = Path(archivo)
    archivo.parent.mkdir(parents=True, exist_ok=True)
    campos = ["case_id", "case_name", "status", "priority", "total_files", "total_subtasks",
              "completed_subtasks", "failed_subtasks", "cancelled_subtasks", "retries", "reassignments",
              "created_at", "finished_at", "total_seconds", "processing_seconds",
              "sequential_seconds", "speedup", "workers", "summary"]
    with open(archivo, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=campos)
        w.writeheader()
        for r in reportes:
            fila = {k: r.get(k) for k in campos}
            fila["workers"] = " ".join(f"{k}:{v['subtasks']}" for k, v in sorted(r["by_worker"].items()))
            w.writerow(fila)

    por_worker = {}
    for r in reportes:
        for wid, d in r["by_worker"].items():
            acc = por_worker.setdefault(wid, {"subtasks": 0, "completed": 0, "failed": 0, "busy_seconds": 0.0})
            for k in acc:
                acc[k] += d[k]
    total_sub = sum(r["total_subtasks"] for r in reportes)
    total_files = sum(r["total_files"] for r in reportes)
    ocupado = sum(d["busy_seconds"] for d in por_worker.values())
    pared = fin_total - inicio_envio
    resumen = {
        "fecha": datetime.now().isoformat(timespec="seconds"),
        "coordinador": COORDINATOR_URL,
        "envios_concurrentes": concurrentes,
        "casos": len(reportes),
        "estados": {s: sum(1 for r in reportes if r["status"] == s) for s in {r["status"] for r in reportes}},
        "archivos": total_files,
        "sub_tareas": total_sub,
        "tiempo_envio_s": round(fin_envio - inicio_envio, 1),
        "tiempo_total_s": round(pared, 1),
        "rendimiento_archivos_por_min": round(total_files / pared * 60, 1) if pared else None,
        "tiempo_secuencial_estimado_s": round(ocupado, 1),
        "speedup_global": round(ocupado / pared, 2) if pared else None,
        "duracion_media_caso_s": round(sum(r["total_seconds"] or 0 for r in reportes) / max(1, len(reportes)), 1),
        "reintentos": sum(r.get("retries", 0) for r in reportes),
        "redistribuciones": sum(r.get("reassignments", 0) for r in reportes),
        "por_worker": {k: {**v, "busy_seconds": round(v["busy_seconds"], 1),
                           "porcentaje_subtareas": round(v["subtasks"] / max(1, total_sub) * 100, 1)}
                       for k, v in sorted(por_worker.items())},
    }
    archivo.with_suffix(".json").write_text(json.dumps(resumen, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n" + "=" * 60)
    print("MÉTRICAS DE LA PRUEBA")
    print("=" * 60)
    print(f"Casos: {resumen['casos']}  {resumen['estados']}")
    print(f"Archivos: {total_files} · Sub-tareas: {total_sub}")
    print(f"Tiempo total: {resumen['tiempo_total_s']} s (envío {resumen['tiempo_envio_s']} s)")
    print(f"Rendimiento: {resumen['rendimiento_archivos_por_min']} archivos/min")
    print(f"Tiempo secuencial estimado: {resumen['tiempo_secuencial_estimado_s']} s → "
          f"speedup global {resumen['speedup_global']}×")
    print(f"Reintentos: {resumen['reintentos']} · Redistribuciones: {resumen['redistribuciones']}")
    print("Reparto por worker:")
    for wid, d in resumen["por_worker"].items():
        print(f"  {wid:20s} {d['subtasks']:4d} sub-tareas ({d['porcentaje_subtareas']}%) · "
              f"ocupado {d['busy_seconds']} s · fallidas {d['failed']}")
    print(f"\nCSV por caso: {archivo}\nResumen JSON: {archivo.with_suffix('.json')}")
    print("=" * 60)


def enviar_lote(grupos, concurrentes, esperar_fin, metricas, prioridad_aleatoria=False):
    """Envía varios casos en paralelo; opcionalmente espera y guarda métricas"""
    print(f"[*] Enviando {len(grupos)} casos con {concurrentes} envíos concurrentes a {COORDINATOR_URL}")
    inicio = time.time()
    ids = []
    with ThreadPoolExecutor(max_workers=concurrentes) as pool:
        futuros = []
        for nombre, archivos, meta in grupos:
            prioridad = random.randint(1, 10) if prioridad_aleatoria else (meta or {}).get("prioridad", 5)
            futuros.append(pool.submit(enviar_archivos, archivos, nombre, prioridad, meta))
        for fut in as_completed(futuros):
            r = fut.result()
            if r:
                ids.append(r["case_id"])
    fin_envio = time.time()
    print(f"\n[✓] {len(ids)}/{len(grupos)} casos enviados en {fin_envio - inicio:.1f} s")

    if esperar_fin or metricas:
        esperar(ids)
        fin = time.time()
        if metricas:
            guardar_metricas(ids, metricas, inicio, fin_envio, fin, concurrentes)
    return ids


# ============================================================
# CONSULTAS
# ============================================================
def consultar_estado(case_id: str):
    try:
        data = requests.get(f"{COORDINATOR_URL}/api/cases/{case_id}", timeout=30).json()
        case = data["case"]
        print(f"\n{'=' * 60}")
        print(f"Caso: {case['case_id']}  ({case['case_name']})")
        print(f"Estado: {case['status']} · prioridad {case['priority']}")
        print(f"Completadas: {case['completed_count']} | Fallidas: {case['failed_count']} | "
              f"Total: {case['total_subtasks']}")
        print("\nSub-tareas:")
        for st in data["subtasks"]:
            prog = f"{st['progress']:.0f}%" if st["status"] in ("assigned", "processing") else ""
            print(f"  {st['subtask_id']} | {st['file_name'][:28]:28s} | {st['operation']:18s} | "
                  f"{str(st.get('pool')):6s} | {st['status']:10s} {prog:4s} | {st['assigned_worker'] or '-'}")
        rep = requests.get(f"{COORDINATOR_URL}/api/cases/{case_id}/report", timeout=30).json()
        print(f"\nResumen: {rep['summary']}")
        print(f"Reporte: {COORDINATOR_URL}/cases/{case_id}/reporte")
        print(f"{'=' * 60}\n")
    except Exception as e:
        print(f"[✗] Error: {e}")


def descargar_resultados(case_id: str, salida: str):
    """Descarga el reporte consolidado y todos los resultados de un caso"""
    rep = requests.get(f"{COORDINATOR_URL}/api/cases/{case_id}/report", timeout=30)
    if rep.status_code != 200:
        print(f"[✗] {rep.text}")
        return
    rep = rep.json()
    destino = Path(salida) / case_id
    destino.mkdir(parents=True, exist_ok=True)
    (destino / f"reporte_{case_id}.json").write_text(json.dumps(rep, ensure_ascii=False, indent=2), encoding="utf-8")
    n = 0
    for f in rep["files"]:
        for st in f["subtasks"]:
            if not st["result_url"]:
                continue
            r = requests.get(COORDINATOR_URL + st["result_url"], timeout=300)
            if r.ok:
                nombre = r.headers.get("content-disposition", "").split("filename=")[-1].strip('"') \
                         or f"{st['subtask_id']}.bin"
                (destino / nombre).write_bytes(r.content)
                n += 1
    print(f"[✓] {n} resultados + reporte guardados en {destino}")
    print(f"    {rep['summary']}")


def cancelar(case_id: str):
    r = requests.post(f"{COORDINATOR_URL}/api/cases/{case_id}/cancel", timeout=30)
    print(("[✓] " if r.ok else "[✗] ") + r.text)


def resumen():
    try:
        stats = requests.get(f"{COORDINATOR_URL}/api/stats", timeout=30).json()
        workers = requests.get(f"{COORDINATOR_URL}/api/workers", timeout=30).json()
        print(f"\n{'=' * 60}\nRESUMEN DEL SISTEMA\n{'=' * 60}")
        print(f"Casos: {stats['cases']}")
        print(f"Sub-tareas: {stats['subtasks']}")
        print(f"Archivos procesados: {stats['files']['procesados']}/{stats['files']['total']}")
        print("Colas:")
        for pool, q in (stats.get("queues") or {}).items():
            if isinstance(q, dict):
                print(f"  {pool:7s} {q['waiting']:4d} en espera · {q['consumers']} worker(s)")
        print("Workers:")
        for w in workers:
            print(f"  {w['worker_id']:16s} | {w['status']:11s} | pools {w.get('pools') or '-':18s} | "
                  f"CPU {w['cpu_usage']}% | RAM {w['memory_usage']}% | activas {w['active_tasks']} | "
                  f"procesadas {w['processed']}")
        print("=" * 60 + "\n")
    except Exception as e:
        print(f"[✗] Error: {e}")


# ============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cliente de la plataforma multimedia")
    sub = parser.add_subparsers(dest="comando")

    p = sub.add_parser("enviar", help="Enviar una carpeta como un caso")
    p.add_argument("carpeta")
    p.add_argument("--nombre")
    p.add_argument("--prioridad", type=int, help="1 (baja) a 10 (alta); por defecto la del metadata.json o 5")

    for nombre, ayuda in (("carga", "Enviar cada sub-carpeta del dataset como un caso, en paralelo"),
                          ("auto", "Agrupar archivos en casos por metadatos y enviarlos")):
        p = sub.add_parser(nombre, help=ayuda)
        p.add_argument("dataset_dir")
        p.add_argument("--concurrentes", type=int, default=3)
        p.add_argument("--esperar", action="store_true", help="esperar a que terminen todos los casos")
        p.add_argument("--metricas", help="guardar métricas en este CSV (implica --esperar)")
        p.add_argument("--prioridad-aleatoria", action="store_true")
        p.add_argument("--max-casos", type=int, help="enviar solo los primeros N casos")
        p.add_argument("--filtro", help="enviar solo los casos cuyo nombre contenga este texto (p. ej. heterogeneo)")
        if nombre == "auto":
            p.add_argument("--por", choices=["carpeta", "evento", "sesion", "usuario", "lote"], default="evento")

    p = sub.add_parser("estado", help="Estado de un caso")
    p.add_argument("case_id")
    p = sub.add_parser("esperar", help="Esperar a que terminen uno o varios casos")
    p.add_argument("case_ids", nargs="+")
    p = sub.add_parser("resultados", help="Descargar reporte y resultados de un caso")
    p.add_argument("case_id")
    p.add_argument("--salida", default="./resultados_descargados")
    p = sub.add_parser("cancelar", help="Cancelar un caso")
    p.add_argument("case_id")
    sub.add_parser("resumen", help="Resumen del sistema")

    args = parser.parse_args()

    if args.comando == "enviar":
        enviar_caso(args.carpeta, args.nombre, args.prioridad)
    elif args.comando in ("carga", "auto"):
        criterio = "carpeta" if args.comando == "carga" else args.por
        grupos = agrupar(Path(args.dataset_dir), criterio)
        if args.filtro:
            grupos = [g for g in grupos if args.filtro.lower() in g[0].lower()]
        if args.max_casos:
            grupos = grupos[:args.max_casos]
        enviar_lote(grupos, args.concurrentes, args.esperar, args.metricas, args.prioridad_aleatoria)
    elif args.comando == "estado":
        consultar_estado(args.case_id)
    elif args.comando == "esperar":
        esperar(args.case_ids)
    elif args.comando == "resultados":
        descargar_resultados(args.case_id, args.salida)
    elif args.comando == "cancelar":
        cancelar(args.case_id)
    elif args.comando == "resumen":
        resumen()
    else:
        parser.print_help()
