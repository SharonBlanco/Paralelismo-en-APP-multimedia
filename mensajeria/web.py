"""
Interfaz web local para probar el importador de chats de WhatsApp.

Usa el mismo código que la plataforma (whatsapp_formato.py + importar.py), así que lo
que se ve aquí es exactamente lo que recibirán el coordinador y los workers.
Solo biblioteca estándar: no requiere instalar nada.

Uso:
  python mensajeria/web.py                    → http://localhost:8100
  python mensajeria/web.py --puerto 9000 --salida dataset_whatsapp

Escucha solo en 127.0.0.1: los chats no salen de esta computadora.
"""

from __future__ import annotations

import argparse
import json
import mimetypes
import re
import shutil
import sys
import tempfile
import threading
import uuid
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

AQUI = Path(__file__).resolve().parent
sys.path.insert(0, str(AQUI))
from importar import ErrorImportacion, importar  # noqa: E402
from whatsapp_formato import parsear_archivo  # noqa: E402
from estadisticas import calcular as calcular_estadisticas  # noqa: E402
from organizacion import ErrorOrganizacion, Organizacion  # noqa: E402

INDEX = AQUI / "web" / "index.html"
SUBIDAS = Path(tempfile.gettempdir()) / "wa_import_web"
LOTE_VALIDO = re.compile(r"^[a-f0-9]{8,32}$")
FECHA_VALIDA = re.compile(r"^\d{4}-\d{2}-\d{2}$")
SIN_CARPETA = "_sin"
MAX_SUBIDA = 4 * 2 ** 30
mimetypes.add_type("audio/ogg", ".opus")
mimetypes.add_type("image/webp", ".webp")

DATASET: Path = Path("dataset_whatsapp")
ORG: Organizacion = Organizacion(DATASET)

# Caché de chats parseados: {caso: (mtime_ns de chat.txt, mensajes)}.
# El panorama general recorre todos los chats; sin caché se releería cada .txt.
_cache: dict[str, tuple[int, list]] = {}
_cache_lock = threading.Lock()


def mensajes_de(carpeta: Path) -> list:
    chat = carpeta / "chat.txt"
    marca = chat.stat().st_mtime_ns
    with _cache_lock:
        guardado = _cache.get(carpeta.name)
        if guardado and guardado[0] == marca:
            return guardado[1]
    mensajes = parsear_archivo(chat)
    with _cache_lock:
        _cache[carpeta.name] = (marca, mensajes)
    return mensajes


def carpetas_de_casos() -> list[Path]:
    if not DATASET.is_dir():
        return []
    return sorted((c for c in DATASET.iterdir() if c.is_dir() and (c / "metadata.json").is_file()),
                  key=lambda p: p.stat().st_mtime, reverse=True)


def dentro(base: Path, ruta: Path) -> bool:
    base, ruta = base.resolve(), ruta.resolve()
    return ruta == base or base in ruta.parents


def resumen_caso(carpeta: Path, asignaciones: dict | None = None) -> dict | None:
    try:
        m = json.loads((carpeta / "metadata.json").read_text(encoding="utf-8"))
        mensajes = mensajes_de(carpeta)
    except (OSError, json.JSONDecodeError):
        return None
    por_dia: dict[str, int] = {}
    for x in mensajes:
        if x.tipo != "sistema":
            por_dia[x.fecha_hora[:10]] = por_dia.get(x.fecha_hora[:10], 0) + 1
    if asignaciones is None:
        asignaciones = ORG.estado()["asignaciones"]
    return {"caso": carpeta.name, "chat": m.get("chat"), "fuente": m.get("fuente"),
            "mensajes": m.get("total_mensajes"), "participantes": len(m.get("participantes", [])),
            "nombres": m.get("participantes", []),
            "inicio": (m.get("periodo") or {}).get("inicio"), "fin": (m.get("periodo") or {}).get("fin"),
            "por_dia": por_dia,     # mensajes de participantes por fecha (búsqueda por fecha)
            "carpeta": asignaciones.get(carpeta.name),
            "importado": (m.get("importado") or {}).get("fecha"), "avisos": len(m.get("avisos", []))}


def estadisticas_generales(carpeta: str, desde: str, hasta: str) -> dict:
    """Estadísticas de todas las conversaciones (opcionalmente de una carpeta y un rango
    de fechas). Mismas métricas que por chat, salvo el tiempo de respuesta."""
    asignaciones = ORG.estado()["asignaciones"]
    todos, por_conversacion, adjuntos = [], [], 0
    for c in carpetas_de_casos():
        asignada = asignaciones.get(c.name)
        if carpeta == SIN_CARPETA and asignada is not None:
            continue
        if carpeta not in ("", SIN_CARPETA) and asignada != carpeta:
            continue
        try:
            meta = json.loads((c / "metadata.json").read_text(encoding="utf-8"))
            mensajes = mensajes_de(c)
        except (OSError, json.JSONDecodeError):
            continue
        elegidos = [m for m in mensajes
                    if (not desde or m.fecha_hora[:10] >= desde) and (not hasta or m.fecha_hora[:10] <= hasta)]
        if not elegidos:
            continue
        todos.extend(elegidos)
        adjuntos += sum(1 for m in elegidos if m.adjunto)
        usuarios = [m for m in elegidos if m.tipo != "sistema"]
        por_conversacion.append({
            "caso": c.name, "chat": meta.get("chat"), "mensajes": len(usuarios),
            "participantes": len({m.autor for m in usuarios}),
            "inicio": min(m.fecha_hora for m in elegidos), "fin": max(m.fecha_hora for m in elegidos)})
    e = calcular_estadisticas(todos, respuestas=False)
    por_conversacion.sort(key=lambda x: -x["mensajes"])
    e["por_conversacion"] = por_conversacion
    e["resumen"]["conversaciones"] = len(por_conversacion)
    e["resumen"]["adjuntos"] = adjuntos
    e["resumen"]["inicio"] = min((m.fecha_hora for m in todos), default=None)
    e["resumen"]["fin"] = max((m.fecha_hora for m in todos), default=None)
    e["filtro"] = {"carpeta": carpeta, "desde": desde, "hasta": hasta}
    return e


class Manejador(BaseHTTPRequestHandler):
    server_version = "ImportadorWA/1.0"

    # ---------------------------------------------------------------- utilidades
    def log_message(self, fmt, *args):
        sys.stderr.write(f"  {self.command} {self.path.split('?')[0]} → {args[1] if len(args) > 1 else ''}\n")

    def _json(self, datos, estado=200):
        cuerpo = json.dumps(datos, ensure_ascii=False).encode()
        self.send_response(estado)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(cuerpo)))
        self.end_headers()
        self.wfile.write(cuerpo)

    def _error(self, mensaje, estado=400):
        self._json({"error": mensaje}, estado)

    def _leer_json(self) -> dict:
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n) or b"{}")

    def _ruta(self):
        u = urlparse(self.path)
        return [unquote(p) for p in u.path.strip("/").split("/") if p], parse_qs(u.query)

    def _caso(self, nombre: str) -> Path | None:
        carpeta = DATASET / nombre
        return carpeta if dentro(DATASET, carpeta) and (carpeta / "metadata.json").is_file() else None

    # ---------------------------------------------------------------- GET
    def do_GET(self):
        partes, qs = self._ruta()
        if not partes:
            cuerpo = INDEX.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(cuerpo)))
            self.end_headers()
            self.wfile.write(cuerpo)
        elif partes == ["api", "casos"]:
            org = ORG.estado()
            casos = [r for c in carpetas_de_casos() if (r := resumen_caso(c, org["asignaciones"]))]
            conteo: dict[str, int] = {}
            for r in casos:
                conteo[r["carpeta"] or SIN_CARPETA] = conteo.get(r["carpeta"] or SIN_CARPETA, 0) + 1
            carpetas = [{**c, "conversaciones": conteo.get(c["id"], 0)}
                        for c in sorted(org["carpetas"], key=lambda c: c["nombre"].lower())]
            self._json({"carpeta": str(DATASET.resolve()), "casos": casos, "carpetas": carpetas,
                        "sin_carpeta": conteo.get(SIN_CARPETA, 0)})
        elif partes == ["api", "general"]:
            carpeta = (qs.get("carpeta") or [""])[0]
            desde, hasta = (qs.get("desde") or [""])[0], (qs.get("hasta") or [""])[0]
            if any(f and not FECHA_VALIDA.match(f) for f in (desde, hasta)):
                return self._error("Fecha inválida (use AAAA-MM-DD)")
            self._json(estadisticas_generales(carpeta, desde, hasta))
        elif len(partes) == 3 and partes[:2] == ["api", "casos"]:
            carpeta = self._caso(partes[2])
            if not carpeta:
                return self._error("Caso no encontrado", 404)
            meta = json.loads((carpeta / "metadata.json").read_text(encoding="utf-8"))
            parseados = mensajes_de(carpeta)
            mensajes = [m.a_dict() for m in parseados]
            for m in mensajes:
                m.pop("linea_cruda", None)
            self._json({"caso": carpeta.name, "metadata": meta, "mensajes": mensajes,
                        "estadisticas": calcular_estadisticas(parseados),
                        "carpeta": ORG.carpeta_de(carpeta.name)})
        elif len(partes) == 3 and partes[0] == "media":
            carpeta = self._caso(partes[1])
            archivo = carpeta / partes[2] if carpeta else None
            if not archivo or not dentro(carpeta, archivo) or not archivo.is_file():
                return self._error("Archivo no encontrado", 404)
            self._enviar_archivo(archivo)
        else:
            self._error("No existe", 404)

    def _enviar_archivo(self, archivo: Path):
        """Soporta Range para poder adelantar audios y videos."""
        total = archivo.stat().st_size
        inicio, fin = 0, total - 1
        rango = re.match(r"bytes=(\d*)-(\d*)", self.headers.get("Range", ""))
        if rango and (rango.group(1) or rango.group(2)):
            if rango.group(1):
                inicio = int(rango.group(1))
                fin = int(rango.group(2)) if rango.group(2) else fin
            else:
                inicio = max(0, total - int(rango.group(2)))
            fin = min(fin, total - 1)
            self.send_response(HTTPStatus.PARTIAL_CONTENT)
            self.send_header("Content-Range", f"bytes {inicio}-{fin}/{total}")
        else:
            self.send_response(200)
        tipo = mimetypes.guess_type(archivo.name)[0] or "application/octet-stream"
        self.send_header("Content-Type", tipo)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Length", str(fin - inicio + 1))
        self.end_headers()
        with open(archivo, "rb") as f:
            f.seek(inicio)
            restante = fin - inicio + 1
            while restante > 0 and (bloque := f.read(min(1024 * 1024, restante))):
                try:
                    self.wfile.write(bloque)
                except (BrokenPipeError, ConnectionResetError):
                    return
                restante -= len(bloque)

    # ---------------------------------------------------------------- POST
    def do_POST(self):
        partes, qs = self._ruta()
        try:
            if partes == ["api", "subir"]:
                return self._subir(qs)
            if partes == ["api", "importar"]:
                return self._importar(self._leer_json())
            if partes == ["api", "pegar"]:
                return self._pegar(self._leer_json())
            if partes == ["api", "carpetas"]:
                return self._json(ORG.crear(self._leer_json().get("nombre", "")))
            if len(partes) == 4 and partes[:2] == ["api", "carpetas"] and partes[3] == "renombrar":
                return self._json(ORG.renombrar(partes[2], self._leer_json().get("nombre", "")))
            if partes == ["api", "mover"]:
                d = self._leer_json()
                casos = [c for c in d.get("casos") or [] if self._caso(str(c))]
                if not casos:
                    return self._error("No se indicaron conversaciones válidas")
                ORG.mover(casos, d.get("carpeta") or None)
                return self._json({"ok": True, "movidas": len(casos)})
        except (ErrorImportacion, ErrorOrganizacion) as e:
            return self._error(str(e))
        except Exception as e:  # noqa: BLE001 — se muestra en la interfaz
            return self._error(f"Error interno: {type(e).__name__}: {e}", 500)
        self._error("No existe", 404)

    def _subir(self, qs):
        """Recibe un archivo crudo (body = bytes) y lo guarda en la carpeta del lote."""
        lote = (qs.get("lote") or [""])[0]
        nombre = Path((qs.get("archivo") or [""])[0]).name
        if not LOTE_VALIDO.match(lote) or not nombre or nombre.startswith("."):
            return self._error("Lote o nombre de archivo inválido")
        n = int(self.headers.get("Content-Length") or 0)
        if n > MAX_SUBIDA:
            return self._error("Archivo demasiado grande", 413)
        carpeta = SUBIDAS / lote
        carpeta.mkdir(parents=True, exist_ok=True)
        with open(carpeta / nombre, "wb") as f:
            restante = n
            while restante > 0 and (bloque := self.rfile.read(min(1024 * 1024, restante))):
                f.write(bloque)
                restante -= len(bloque)
        self._json({"ok": True, "archivo": nombre, "bytes": n})

    def _ejecutar_importacion(self, entrada: Path, nombre_entrada: str, d: dict):
        caso = importar(entrada, DATASET, fuente=d.get("fuente") or "manual",
                        declaro=bool(d.get("declaro")), nombre=(d.get("nombre") or "").strip() or None,
                        prioridad=int(d.get("prioridad") or 5), sobrescribir=bool(d.get("sobrescribir")),
                        nombre_entrada=nombre_entrada)
        if d.get("carpeta"):            # sin carpeta elegida, un caso reemplazado conserva la suya
            ORG.mover([caso.name], d["carpeta"])
        self._json({"caso": caso.name, "resumen": resumen_caso(caso)})

    def _importar(self, d: dict):
        lote = d.get("lote", "")
        if not LOTE_VALIDO.match(lote) or not (SUBIDAS / lote).is_dir():
            return self._error("No hay archivos subidos para importar")
        carpeta = SUBIDAS / lote
        try:
            archivos = [p for p in carpeta.iterdir() if p.is_file()]
            zips = [p for p in archivos if p.suffix.lower() == ".zip"]
            txts = [p for p in archivos if p.suffix.lower() == ".txt"]
            if len(zips) == 1 and len(archivos) == 1:
                entrada, nombre_entrada = zips[0], zips[0].name
            elif len(txts) == 1 and len(archivos) == 1:
                entrada, nombre_entrada = txts[0], txts[0].name
            elif zips and len(archivos) > 1:
                raise ErrorImportacion("Suba el .zip solo, o bien el .txt con sus archivos (no ambos)")
            else:
                entrada, nombre_entrada = carpeta, (txts[0].name if len(txts) == 1 else carpeta.name)
            self._ejecutar_importacion(entrada, nombre_entrada, d)
        finally:
            shutil.rmtree(carpeta, ignore_errors=True)

    def _pegar(self, d: dict):
        texto = d.get("texto") or ""
        if not texto.strip():
            raise ErrorImportacion("El texto está vacío")
        titulo = re.sub(r"[^\w\- ]+", "", (d.get("nombre") or "").strip()) or "texto pegado"
        carpeta = SUBIDAS / uuid.uuid4().hex
        carpeta.mkdir(parents=True)
        try:
            txt = carpeta / f"Chat de WhatsApp con {titulo}.txt"
            txt.write_text(texto, encoding="utf-8")
            self._ejecutar_importacion(txt, txt.name, d)
        finally:
            shutil.rmtree(carpeta, ignore_errors=True)

    # ---------------------------------------------------------------- DELETE
    def do_DELETE(self):
        partes, _ = self._ruta()
        if len(partes) == 3 and partes[:2] == ["api", "casos"]:
            carpeta = self._caso(partes[2])
            if not carpeta:
                return self._error("Caso no encontrado", 404)
            shutil.rmtree(carpeta)
            ORG.olvidar(carpeta.name)
            with _cache_lock:
                _cache.pop(carpeta.name, None)
            return self._json({"ok": True})
        if len(partes) == 3 and partes[:2] == ["api", "carpetas"]:
            try:
                return self._json({"ok": True, "liberadas": ORG.borrar(partes[2])})
            except ErrorOrganizacion as e:
                return self._error(str(e), 404)
        self._error("No existe", 404)


def main():
    global DATASET, ORG
    ap = argparse.ArgumentParser(description="Interfaz web local del importador de WhatsApp")
    ap.add_argument("--puerto", type=int, default=8100)
    ap.add_argument("--salida", type=Path, default=AQUI.parent / "dataset_whatsapp",
                    help="carpeta donde quedan los casos importados")
    a = ap.parse_args()
    DATASET = a.salida.resolve()
    DATASET.mkdir(parents=True, exist_ok=True)
    ORG = Organizacion(DATASET)
    servidor = ThreadingHTTPServer(("127.0.0.1", a.puerto), Manejador)
    print(f"Importador de WhatsApp en http://localhost:{a.puerto}")
    print(f"Casos en {DATASET}   (Ctrl+C para detener)")
    try:
        servidor.serve_forever()
    except KeyboardInterrupt:
        print("\nDetenido.")


if __name__ == "__main__":
    main()
