"""
Parser del formato de exportación de chats de WhatsApp (Android e iOS).

Solo usa la biblioteca estándar, para que lo puedan usar tanto el importador
como el worker de texto sin dependencias extra.

Formatos reconocidos (ejemplos reales de exportación):
  Android 24 h : 10/11/26, 14:05 - Sharon: hola
  Android 12 h : 10/11/26, 2:05 p. m. - Sharon: hola      (con espacios U+202F / U+00A0)
  iOS          : [10/11/26, 2:05:32 p. m.] Sharon: hola
  Sistema      : 10/11/26, 14:05 - Sharon creó el grupo "X"   (sin autor)

Adjuntos (exportación "Incluir archivos"):
  Android : IMG-20261110-WA0001.jpg (archivo adjunto)  /  (file attached)
  iOS     : <adjunto: 00000012-PHOTO-2026-11-10-14-05-32.jpg>  /  <attached: ...>
Adjuntos omitidos (exportación "Sin archivos" o archivo no descargado):
  <Multimedia omitido>, <Media omitted>, imagen omitida, audio omitido...

Regex de partida tomada de AnalyzeWhatsApp.py (basado en
https://www.mastros.online/blog/analyze-whatsapp-chats-in-python) y ampliada.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, asdict, field
from datetime import datetime
from pathlib import Path

# Caracteres invisibles que WhatsApp mete en las exportaciones
LRM = "‎"          # marca izquierda-a-derecha (antes de adjuntos y en iOS)
ESPACIOS = " \t  "

PATRON_LINEA = re.compile(
    rf"^[{LRM}]?\[?(?P<fecha>\d{{1,2}}/\d{{1,2}}/\d{{2,4}}),?[{ESPACIOS}]+"
    rf"(?P<hora>\d{{1,2}}:\d{{2}}(?::\d{{2}})?)"
    rf"(?:[{ESPACIOS}]*(?P<ampm>[ap]\.?[{ESPACIOS}]?m\.?|AM|PM))?"
    rf"\]?[{ESPACIOS}]*(?:-[{ESPACIOS}]*)?"
    r"(?P<resto>.*)$",
    re.IGNORECASE,
)
PATRON_AUTOR = re.compile(r"^(?P<autor>[^:]+?):\s(?P<mensaje>.*)$", re.DOTALL)

ADJUNTO_ANDROID = re.compile(
    rf"^[{LRM}]?(?P<archivo>[^\n]+?\.[A-Za-z0-9]{{2,5}})[{ESPACIOS}]+"
    r"\((?:archivo adjunto|file attached|arquivo anexado)\)",
    re.IGNORECASE,
)
ADJUNTO_IOS = re.compile(
    rf"[{LRM}]?<(?:adjunto|attached|anexado):[{ESPACIOS}]*(?P<archivo>[^>]+)>",
    re.IGNORECASE,
)

MARCAS_OMITIDO = ["<multimedia omitido>", "<media omitted>", "imagen omitida", "image omitted",
                  "video omitido", "video omitted", "audio omitido", "audio omitted",
                  "gif omitido", "gif omitted", "sticker omitido", "sticker omitted",
                  "documento omitido", "document omitted", "<archivo omitido>"]
MARCAS_ELIMINADO = ["se eliminó este mensaje", "eliminaste este mensaje", "this message was deleted",
                    "you deleted this message"]

EXT_IMAGEN = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".heic"}
EXT_AUDIO = {".opus", ".ogg", ".mp3", ".m4a", ".aac", ".wav", ".amr"}
EXT_VIDEO = {".mp4", ".3gp", ".mov", ".mkv", ".avi"}


def tipo_archivo(nombre: str) -> str:
    """Clasifica un adjunto por extensión. Los PTT-*.opus son notas de voz."""
    ext = Path(nombre).suffix.lower()
    if ext in EXT_IMAGEN:
        return "imagen"
    if ext in EXT_AUDIO:
        return "voz" if Path(nombre).name.upper().startswith("PTT-") or "AUDIO" in nombre.upper() else "audio"
    if ext in EXT_VIDEO:
        return "video"
    return "documento"


@dataclass
class Mensaje:
    id: int                      # 1, 2, 3... en el orden del archivo (identifica la evidencia)
    linea: int                   # línea del .txt donde empieza (enlace a la fuente original)
    fecha_hora: str              # ISO 8601, sin zona horaria (la del teléfono que exportó)
    autor: str | None            # None = mensaje del sistema
    texto: str                   # texto del mensaje (sin el marcador del adjunto)
    tipo: str                    # texto | imagen | voz | audio | video | documento | omitido | eliminado | sistema
    adjunto: str | None = None   # nombre del archivo adjunto, si lo hay
    linea_cruda: str = field(default="", repr=False)

    def a_dict(self) -> dict:
        return asdict(self)


def _a_24h(hora: str, ampm: str | None) -> str:
    """Convierte la hora de 12 h (a. m./p. m.) a 24 h."""
    if not ampm:
        return hora
    partes = hora.split(":")
    h = int(partes[0])
    es_pm = re.sub(rf"[.{ESPACIOS}]", "", ampm.lower()).startswith("p")
    if es_pm and h != 12:
        h += 12
    if not es_pm and h == 12:
        h = 0
    partes[0] = f"{h:02d}"
    return ":".join(partes)


def _fecha(fecha: str, hora: str, dia_primero: bool) -> datetime | None:
    a, b, anio = (int(x) for x in fecha.split("/"))
    dia, mes = (a, b) if dia_primero else (b, a)
    if anio < 100:
        anio += 2000
    h, m, *s = (int(x) for x in hora.split(":"))
    try:
        return datetime(anio, mes, dia, h, m, s[0] if s else 0)
    except ValueError:
        return None


def _clasificar(autor: str | None, texto: str) -> tuple[str, str | None, str]:
    """Devuelve (tipo, adjunto, texto_sin_marcador)."""
    if autor is None:
        return "sistema", None, texto
    for patron in (ADJUNTO_ANDROID, ADJUNTO_IOS):
        m = patron.search(texto)
        if m:
            archivo = m.group("archivo").strip().strip(LRM)
            resto = (texto[:m.start()] + texto[m.end():]).strip().strip(LRM).strip()
            return tipo_archivo(archivo), archivo, resto   # resto = pie de foto, si lo hay
    bajo = texto.lower()
    if any(m in bajo for m in MARCAS_OMITIDO):
        return "omitido", None, texto
    if any(m in bajo for m in MARCAS_ELIMINADO):
        return "eliminado", None, texto
    return "texto", None, texto


def detectar_dia_primero(lineas: list[str]) -> bool:
    """Si algún primer campo de la fecha es > 12 es día/mes; si el segundo lo es, mes/día.
    Si no se puede decidir, se asume día/mes (configuración regional de Costa Rica)."""
    for linea in lineas:
        m = PATRON_LINEA.match(linea)
        if m:
            a, b, _ = (int(x) for x in m.group("fecha").split("/"))
            if a > 12:
                return True
            if b > 12:
                return False
    return True


def parsear_lineas(lineas: list[str], dia_primero: bool | None = None) -> list[Mensaje]:
    """Convierte las líneas del .txt en mensajes. Las líneas sin marca de tiempo se
    pegan al mensaje anterior (mensajes multilínea)."""
    lineas = [l.rstrip("\r\n") for l in lineas]
    if dia_primero is None:
        dia_primero = detectar_dia_primero(lineas)

    crudos = []   # [linea_inicio, fecha, autor, texto, linea_cruda]
    for n, linea in enumerate(lineas, start=1):
        m = PATRON_LINEA.match(linea)
        fh = _fecha(m.group("fecha"), _a_24h(m.group("hora"), m.group("ampm")), dia_primero) if m else None
        if fh:
            resto = m.group("resto")
            ma = PATRON_AUTOR.match(resto)
            autor, texto = (ma.group("autor").strip(LRM).strip(), ma.group("mensaje")) if ma else (None, resto)
            crudos.append([n, fh, autor, texto, linea])
        elif crudos:
            crudos[-1][3] += "\n" + linea
            crudos[-1][4] += "\n" + linea

    mensajes = []
    for i, (n, fh, autor, texto, cruda) in enumerate(crudos, start=1):
        tipo, adjunto, texto = _clasificar(autor, texto)
        mensajes.append(Mensaje(i, n, fh.isoformat(), autor, texto, tipo, adjunto, cruda))
    return mensajes


def parsear_archivo(ruta: str | Path, dia_primero: bool | None = None) -> list[Mensaje]:
    # utf-8-sig quita el BOM que a veces trae la exportación
    with open(ruta, encoding="utf-8-sig", errors="replace") as f:
        return parsear_lineas(f.readlines(), dia_primero)


def es_chat_whatsapp(ruta: str | Path, minimo: int = 3) -> bool:
    """Heurística: las primeras líneas con contenido deben tener la marca de tiempo."""
    try:
        with open(ruta, encoding="utf-8-sig", errors="replace") as f:
            lineas = [l for _, l in zip(range(50), f) if l.strip()]
    except OSError:
        return False
    return sum(bool(PATRON_LINEA.match(l)) for l in lineas) >= min(minimo, len(lineas)) > 0
