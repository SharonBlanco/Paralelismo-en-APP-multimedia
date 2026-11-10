"""
Carpetas para organizar las conversaciones importadas.

Son carpetas lógicas: se guardan en <dataset>/.organizacion.json y no mueven las
carpetas de los casos en disco. Así la ruta de cada caso (la que usa cliente.py para
enviarlo) y su metadata.json (evidencia, con SHA-256) no cambian al reorganizar.

Formato:
  {"carpetas": [{"id": "a1b2c3d4", "nombre": "Pruebas", "creada": "2026-11-10T11:00:00"}],
   "asignaciones": {"<caso>": "<id de carpeta>"}}
"""

from __future__ import annotations

import json
import os
import threading
import uuid
from datetime import datetime
from pathlib import Path

ARCHIVO = ".organizacion.json"
_lock = threading.Lock()


class ErrorOrganizacion(Exception):
    pass


class Organizacion:
    def __init__(self, dataset: Path):
        self.ruta = dataset / ARCHIVO

    # ------------------------------------------------------------ persistencia
    def _leer(self) -> dict:
        try:
            datos = json.loads(self.ruta.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            datos = {}
        datos.setdefault("carpetas", [])
        datos.setdefault("asignaciones", {})
        return datos

    def _escribir(self, datos: dict) -> None:
        # escritura atómica: si el proceso muere a mitad, el archivo anterior sigue intacto
        tmp = self.ruta.with_suffix(".tmp")
        tmp.write_text(json.dumps(datos, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, self.ruta)

    # ------------------------------------------------------------ consultas
    def estado(self) -> dict:
        with _lock:
            return self._leer()

    def carpeta_de(self, caso: str) -> str | None:
        return self.estado()["asignaciones"].get(caso)

    # ------------------------------------------------------------ cambios
    @staticmethod
    def _nombre_valido(nombre: str) -> str:
        nombre = (nombre or "").strip()
        if not nombre:
            raise ErrorOrganizacion("El nombre de la carpeta está vacío")
        if len(nombre) > 60:
            raise ErrorOrganizacion("El nombre de la carpeta es demasiado largo (máximo 60)")
        return nombre

    def crear(self, nombre: str) -> dict:
        nombre = self._nombre_valido(nombre)
        with _lock:
            d = self._leer()
            if any(c["nombre"].lower() == nombre.lower() for c in d["carpetas"]):
                raise ErrorOrganizacion(f'Ya existe una carpeta llamada "{nombre}"')
            carpeta = {"id": uuid.uuid4().hex[:8], "nombre": nombre,
                       "creada": datetime.now().isoformat(timespec="seconds")}
            d["carpetas"].append(carpeta)
            self._escribir(d)
            return carpeta

    def renombrar(self, id_: str, nombre: str) -> dict:
        nombre = self._nombre_valido(nombre)
        with _lock:
            d = self._leer()
            carpeta = next((c for c in d["carpetas"] if c["id"] == id_), None)
            if not carpeta:
                raise ErrorOrganizacion("Carpeta no encontrada")
            if any(c["nombre"].lower() == nombre.lower() and c["id"] != id_ for c in d["carpetas"]):
                raise ErrorOrganizacion(f'Ya existe una carpeta llamada "{nombre}"')
            carpeta["nombre"] = nombre
            self._escribir(d)
            return carpeta

    def borrar(self, id_: str) -> int:
        """Borra la carpeta; sus conversaciones quedan "sin carpeta" (no se borran)."""
        with _lock:
            d = self._leer()
            if not any(c["id"] == id_ for c in d["carpetas"]):
                raise ErrorOrganizacion("Carpeta no encontrada")
            d["carpetas"] = [c for c in d["carpetas"] if c["id"] != id_]
            liberados = [k for k, v in d["asignaciones"].items() if v == id_]
            for k in liberados:
                del d["asignaciones"][k]
            self._escribir(d)
            return len(liberados)

    def mover(self, casos: list[str], id_: str | None) -> None:
        """Asigna los casos a la carpeta id_ (None = sin carpeta)."""
        with _lock:
            d = self._leer()
            if id_ is not None and not any(c["id"] == id_ for c in d["carpetas"]):
                raise ErrorOrganizacion("Carpeta no encontrada")
            for caso in casos:
                if id_ is None:
                    d["asignaciones"].pop(caso, None)
                else:
                    d["asignaciones"][caso] = id_
            self._escribir(d)

    def olvidar(self, caso: str) -> None:
        """Al borrar un caso se quita su asignación."""
        with _lock:
            d = self._leer()
            if d["asignaciones"].pop(caso, None) is not None:
                self._escribir(d)
