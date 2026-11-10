"""
Estadísticas de una conversación, calculadas a partir de los mensajes parseados.

Mismas métricas que analyze() de AnalyzeWhatsApp.py, pero solo con la biblioteca
estándar (sin pandas), para que las sirva la interfaz web y el worker de texto.
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from statistics import median
from urllib.parse import urlparse

from whatsapp_formato import Mensaje

# Palabras vacías básicas en español e inglés (compartidas con AnalyzeWhatsApp.py)
STOPWORDS = set("""
a al algo ante antes así aun aunque bien cada como con contra cual cuando de del desde donde
dos e el ella ellas ellos en entre era es esa ese eso esta este esto estoy está están fue
ha hay he la las le les lo los me mi mis muy más ni no nos o os para pero por porque que qué
se si sí sin sobre son su sus también te ti tu tus un una uno unos y ya yo vos usted ustedes
jaja jajaja jajajaja xd ok q pq tmb voy va ver bueno pues eh ah oh
the and to of i you it is a in that for on my me be are was this with so just do not have
""".split())

URL = re.compile(r"https?://\S+")
NO_PALABRA = re.compile(r"[^\wáéíóúüñ\s]", re.IGNORECASE)
MULTIMEDIA = {"imagen", "voz", "audio", "video", "documento", "omitido"}
RESPUESTA_MAX_MIN = 12 * 60   # una "respuesta" a más de 12 h no cuenta (mismo criterio que AnalyzeWhatsApp)


def tokens(texto: str) -> list[str]:
    limpio = NO_PALABRA.sub(" ", URL.sub(" ", texto).lower())
    return [p for p in limpio.split() if p not in STOPWORDS and len(p) > 2 and not p.isdigit()]


def calcular(mensajes: list[Mensaje], respuestas: bool = True) -> dict:
    """respuestas=False omite el tiempo de respuesta: al juntar varios chats, el mensaje
    "siguiente" puede ser de otra conversación y la medida no tendría sentido."""
    usuarios = [m for m in mensajes if m.tipo != "sistema"]
    # clave por objeto y no por m.id: al combinar chats los ids se repiten
    fechas = {id(m): datetime.fromisoformat(m.fecha_hora) for m in mensajes}

    # ---- por participante
    por_autor: dict[str, dict] = {}
    for m in usuarios:
        a = por_autor.setdefault(m.autor, {"autor": m.autor, "mensajes": 0, "palabras": 0, "textos": 0,
                                           "multimedia": 0, "primero": m.fecha_hora, "ultimo": m.fecha_hora})
        a["mensajes"] += 1
        if m.tipo == "texto":
            a["textos"] += 1
            a["palabras"] += len(m.texto.split())
        elif m.tipo in MULTIMEDIA:
            a["multimedia"] += 1
        a["primero"] = min(a["primero"], m.fecha_hora)
        a["ultimo"] = max(a["ultimo"], m.fecha_hora)

    # tiempo de respuesta: minutos entre mensajes consecutivos de autores distintos
    esperas = defaultdict(list)
    orden = sorted(usuarios, key=lambda m: (m.fecha_hora, m.id)) if respuestas else []
    for previo, actual in zip(orden, orden[1:]):
        if actual.autor != previo.autor:
            minutos = (fechas[id(actual)] - fechas[id(previo)]).total_seconds() / 60
            if 0 <= minutos < RESPUESTA_MAX_MIN:
                esperas[actual.autor].append(minutos)

    total_usuarios = len(usuarios) or 1
    participantes = []
    for a in por_autor.values():
        participantes.append({
            "autor": a["autor"], "mensajes": a["mensajes"],
            "porcentaje": round(100 * a["mensajes"] / total_usuarios, 1),
            "palabras_promedio": round(a["palabras"] / a["textos"], 1) if a["textos"] else 0,
            "multimedia": a["multimedia"],
            "respuesta_mediana_min": round(median(esperas[a["autor"]]), 1) if esperas[a["autor"]] else None,
            "respuestas": len(esperas[a["autor"]]),
            "primero": a["primero"], "ultimo": a["ultimo"],
        })
    participantes.sort(key=lambda p: -p["mensajes"])

    # ---- actividad temporal (días sin mensajes incluidos con 0)
    por_dia = Counter(m.fecha_hora[:10] for m in usuarios)
    serie_dias = []
    if por_dia:
        d, fin = datetime.fromisoformat(min(por_dia)), datetime.fromisoformat(max(por_dia))
        while d <= fin:
            k = d.date().isoformat()
            serie_dias.append({"fecha": k, "mensajes": por_dia.get(k, 0)})
            d += timedelta(days=1)

    calor = [[0] * 24 for _ in range(7)]          # [día de la semana 0=lunes][hora]
    for m in usuarios:
        f = fechas[id(m)]
        calor[f.weekday()][f.hour] += 1
    por_hora = [sum(calor[d][h] for d in range(7)) for h in range(24)]
    por_dia_semana = [sum(fila) for fila in calor]

    # ---- contenido
    palabras = Counter(t for m in usuarios if m.tipo == "texto" for t in tokens(m.texto))
    dominios = Counter(urlparse(u).netloc.replace("www.", "")
                       for m in usuarios for u in URL.findall(m.texto))
    por_tipo = Counter(m.tipo for m in mensajes)

    hora_pico = max(range(24), key=lambda h: por_hora[h]) if usuarios else None
    dia_pico = max(serie_dias, key=lambda x: x["mensajes"]) if serie_dias else None
    return {
        "resumen": {
            "mensajes": len(mensajes),
            "mensajes_usuarios": len(usuarios),
            "participantes": len(participantes),
            "dias_periodo": len(serie_dias),
            "dias_activos": sum(1 for x in serie_dias if x["mensajes"]),
            "promedio_diario": round(len(usuarios) / len(serie_dias), 1) if serie_dias else 0,
            "adjuntos": sum(1 for m in mensajes if m.adjunto),
            "hora_pico": hora_pico,
            "dia_pico": dia_pico,
        },
        "participantes": participantes,
        "por_dia": serie_dias,
        "por_hora": por_hora,
        "por_dia_semana": por_dia_semana,
        "calor": calor,
        "por_tipo": dict(por_tipo.most_common()),
        "palabras": palabras.most_common(25),
        "dominios": dominios.most_common(10),
    }
