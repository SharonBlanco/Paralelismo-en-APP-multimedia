"""
Importador de exportaciones de WhatsApp → carpeta de caso de la plataforma.

Acepta lo que produce WhatsApp al exportar un chat:
  - el .zip de "Incluir archivos" (Android: "WhatsApp Chat with X.zip"; iOS: contiene _chat.txt)
  - un .txt suelto ("Sin archivos")
  - una carpeta con el .txt y los adjuntos ya descomprimidos

Genera  <salida>/<nombre>/  con:
  chat.txt        el .txt original, sin modificar (cadena de custodia)
  <adjuntos>      cada archivo multimedia referenciado en el chat
  metadata.json   procedencia, declaración de consentimiento, participantes,
                  SHA-256 de cada archivo y, por adjunto, el mensaje que lo contiene

La carpeta resultante se envía con:  python cliente/cliente.py enviar <salida>/<nombre>

Uso:
  python mensajeria/importar.py "WhatsApp Chat with Proyecto SO.zip" --fuente manual
  python mensajeria/importar.py chat.zip --fuente telefono --declaro --nombre caso_demo

Ética (enunciado, B.3): solo se importan chats creados por el equipo con contenido
ficticio. Para fuentes distintas de "generador" se exige la declaración explícita
(--declaro o confirmación interactiva) y queda registrada en metadata.json.
"""

from __future__ import annotations

import argparse
import getpass
import hashlib
import json
import re
import shutil
import sys
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from whatsapp_formato import es_chat_whatsapp, parsear_archivo  # noqa: E402

MAX_ZIP_BYTES = 8 * 2 ** 30          # tope de descompresión (protege contra zip bombs)
PATRON_TELEFONO = re.compile(r"^\+?[\d\s\-()]{7,}$")
PATRONES_NOMBRE = [re.compile(p, re.IGNORECASE) for p in (
    r"^Chat de WhatsApp con (.+)$", r"^WhatsApp Chat with (.+)$", r"^WhatsApp Chat - (.+)$",
    r"^Conversa do WhatsApp com (.+)$")]

class ErrorImportacion(Exception):
    """Error esperado (entrada inválida, falta declaración...). La CLI lo muestra y sale;
    la interfaz web lo devuelve como mensaje."""


TEXTO_DECLARACION = ("Declaro que todos los participantes de este chat son integrantes del equipo, "
                     "que dieron su consentimiento y que el contenido es ficticio (enunciado, B.3).")


def sha256(ruta: Path) -> str:
    h = hashlib.sha256()
    with open(ruta, "rb") as f:
        while bloque := f.read(1024 * 1024):
            h.update(bloque)
    return h.hexdigest()


def extraer_zip(zip_path: Path, destino: Path) -> None:
    """Descomprime validando rutas (zip slip) y tamaño total."""
    with zipfile.ZipFile(zip_path) as z:
        total = sum(i.file_size for i in z.infolist())
        if total > MAX_ZIP_BYTES:
            raise ErrorImportacion(f"El zip descomprimido ocuparía {total / 2**30:.1f} GB (máximo {MAX_ZIP_BYTES / 2**30:.0f} GB)")
        base = destino.resolve()
        for info in z.infolist():
            final = (destino / info.filename).resolve()
            if base not in final.parents and final != base:
                raise ErrorImportacion(f"Ruta sospechosa dentro del zip: {info.filename}")
        z.extractall(destino)


def buscar_chat(carpeta: Path) -> Path:
    candidatos = [p for p in carpeta.rglob("*.txt") if es_chat_whatsapp(p)]
    if not candidatos:
        raise ErrorImportacion(f"No se encontró un .txt con formato de chat de WhatsApp en {carpeta}")
    if len(candidatos) > 1:
        lista = "\n  ".join(str(c.relative_to(carpeta)) for c in candidatos)
        raise ErrorImportacion(f"Hay varios chats; importe uno a la vez:\n  {lista}")
    return candidatos[0]


def nombre_chat(*rutas: Path) -> str:
    for r in rutas:
        for p in PATRONES_NOMBRE:
            m = p.match(r.stem)
            if m:
                return m.group(1).strip()
    return next((r.stem for r in rutas if r.stem not in ("_chat", "chat")), "chat")


def slug(texto: str) -> str:
    s = re.sub(r"[^\w\-]+", "_", texto.strip().lower(), flags=re.UNICODE).strip("_")
    return s or "chat"


def confirmar_declaracion(fuente: str, declaro: bool, interactivo: bool = False) -> dict | None:
    if fuente == "generador":
        return None
    if not declaro:
        if not (interactivo and sys.stdin.isatty()):
            raise ErrorImportacion("Falta la declaración de consentimiento (en la terminal: --declaro)")
        print(TEXTO_DECLARACION)
        if input("¿Lo confirma? (s/N): ").strip().lower() not in ("s", "si", "sí"):
            raise ErrorImportacion("Importación cancelada.")
    return {"texto": TEXTO_DECLARACION, "declarado_por": getpass.getuser(),
            "fecha": datetime.now().isoformat(timespec="seconds")}


def importar(entrada: Path, salida: Path, fuente: str, declaro: bool,
             nombre: str | None = None, prioridad: int = 5, sobrescribir: bool = False,
             interactivo: bool = False, nombre_entrada: str | None = None) -> Path:
    """nombre_entrada: nombre original del archivo cuando `entrada` es una copia temporal
    (la interfaz web sube los archivos a una carpeta de trabajo)."""
    if not entrada.exists():
        raise ErrorImportacion(f"No existe: {entrada}")
    declaracion = confirmar_declaracion(fuente, declaro, interactivo)

    with tempfile.TemporaryDirectory(prefix="wa_import_") as tmp:
        tmp = Path(tmp)
        if entrada.is_file() and entrada.suffix.lower() == ".zip":
            extraer_zip(entrada, tmp)
            carpeta = tmp
        elif entrada.is_file():
            carpeta = entrada.parent
        else:
            carpeta = entrada
        chat_txt = entrada if entrada.is_file() and entrada.suffix.lower() == ".txt" else buscar_chat(carpeta)
        media_dir = chat_txt.parent

        mensajes = parsear_archivo(chat_txt)
        if not mensajes:
            raise ErrorImportacion(f"{chat_txt.name} no contiene mensajes reconocibles")

        titulo = nombre_chat(chat_txt, Path(nombre_entrada) if nombre_entrada else entrada)
        caso = salida / slug(nombre or titulo)
        if caso.exists():
            if not sobrescribir:
                raise ErrorImportacion(f"Ya existe {caso} (use --sobrescribir)")
            shutil.rmtree(caso)
        caso.mkdir(parents=True)

        shutil.copy2(chat_txt, caso / "chat.txt")

        files_meta = {"chat.txt": {"tipo": "chat", "sha256": sha256(caso / "chat.txt"),
                                   "nombre_original": chat_txt.name, "mensajes": len(mensajes)}}
        faltantes = []
        for m in mensajes:
            if not m.adjunto:
                continue
            origen = media_dir / m.adjunto
            if not origen.is_file():
                faltantes.append({"archivo": m.adjunto, "mensaje_id": m.id, "linea": m.linea})
                continue
            destino = caso / Path(m.adjunto).name
            shutil.copy2(origen, destino)
            files_meta[destino.name] = {
                "tipo": m.tipo, "sha256": sha256(destino), "bytes": destino.stat().st_size,
                "mensaje_id": m.id, "linea": m.linea, "fecha_hora": m.fecha_hora,
                "autor": m.autor, "pie": m.texto or None,
            }

        referenciados = {m.adjunto for m in mensajes if m.adjunto}
        # Con un .txt suelto, su carpeta puede tener cualquier cosa: no se reporta
        no_referenciados = [] if chat_txt == entrada else sorted(
            p.name for p in media_dir.iterdir()
            if p.is_file() and p != chat_txt and p.name not in referenciados)

    autores = sorted({m.autor for m in mensajes if m.autor})
    telefonos = [a for a in autores if PATRON_TELEFONO.match(a)]
    conteo = {}
    for m in mensajes:
        conteo[m.tipo] = conteo.get(m.tipo, 0) + 1

    meta = {
        "case_name": caso.name,
        "origen": "exportacion_whatsapp",
        "fuente": fuente,                      # manual | telefono | generador
        "chat": titulo,
        "prioridad": prioridad,
        "declaracion": declaracion,
        "importado": {"fecha": datetime.now().isoformat(timespec="seconds"),
                      "archivo_entrada": nombre_entrada or entrada.name,
                      "sha256_entrada": sha256(entrada) if entrada.is_file() else None},
        "participantes": autores,
        # min/max y no primero/último: WhatsApp no garantiza orden cronológico
        # (p. ej. "Se te añadió al grupo" puede quedar con hora posterior a los mensajes)
        "periodo": {"inicio": min(m.fecha_hora for m in mensajes),
                    "fin": max(m.fecha_hora for m in mensajes)},
        "total_mensajes": len(mensajes),
        "mensajes_por_tipo": conteo,
        "adjuntos_faltantes": faltantes,       # referenciados en el chat pero no incluidos
        "archivos_no_referenciados": no_referenciados,
        "file_count": len(files_meta),
        "files": files_meta,
    }

    avisos = []
    if conteo.get("omitido"):
        avisos.append(f"{conteo['omitido']} adjuntos aparecen como omitidos: se exportó 'Sin archivos' "
                      "o no estaban descargados en el teléfono.")
    if faltantes:
        avisos.append(f"{len(faltantes)} adjuntos están referenciados en el chat pero no venían en la exportación.")
    if telefonos:
        avisos.append(f"{len(telefonos)} participante(s) aparecen como número de teléfono: "
                      "use contactos con nombre ficticio para no exponer números reales.")
    meta["avisos"] = avisos
    (caso / "metadata.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[✓] {titulo} → {caso}")
    print(f"    {len(mensajes)} mensajes de {len(autores)} participantes "
          f"({meta['periodo']['inicio'][:10]} a {meta['periodo']['fin'][:10]})")
    print(f"    tipos: " + ", ".join(f"{k} {v}" for k, v in sorted(conteo.items())))
    print(f"    adjuntos copiados: {len(files_meta) - 1}, faltantes: {len(faltantes)}")
    for a in avisos:
        print(f"[!] {a}")
    return caso


def main():
    ap = argparse.ArgumentParser(description="Importa una exportación de WhatsApp como caso de la plataforma")
    ap.add_argument("entrada", type=Path, help=".zip, .txt o carpeta exportada de WhatsApp")
    ap.add_argument("--salida", type=Path, default=Path("dataset_whatsapp"))
    ap.add_argument("--fuente", choices=["manual", "telefono", "generador"], default="manual")
    ap.add_argument("--declaro", action="store_true", help="confirma la declaración de consentimiento")
    ap.add_argument("--nombre", help="nombre de la carpeta del caso (por defecto, el del chat)")
    ap.add_argument("--prioridad", type=int, default=5)
    ap.add_argument("--sobrescribir", action="store_true")
    a = ap.parse_args()
    try:
        importar(a.entrada, a.salida, a.fuente, a.declaro, a.nombre, a.prioridad, a.sobrescribir,
                 interactivo=True)
    except ErrorImportacion as e:
        sys.exit(f"[✗] {e}")


if __name__ == "__main__":
    main()
