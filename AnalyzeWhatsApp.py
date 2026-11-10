"""
Análisis de chats de WhatsApp en Python
Basado en el flujo de https://www.mastros.online/blog/analyze-whatsapp-chats-in-python

Uso:
    pip install pandas matplotlib seaborn emoji wordcloud
    (opcional, para sentimiento en inglés) pip install vaderSentiment
    python AnalyzeWhatsApp.py chat.txt

Cómo exportar el chat: WhatsApp > abrir el chat > ⋮ > Más > Exportar chat > "Sin archivos".
Los resultados (gráficos PNG y un CSV limpio) se guardan en la carpeta ./resultados
"""

import sys
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import matplotlib
matplotlib.use("Agg")  # guarda imágenes sin abrir ventanas
import matplotlib.pyplot as plt
import seaborn as sns
import emoji
from wordcloud import WordCloud

SALIDA = Path("resultados")

# ---------------------------------------------------------------------------
# 1. PARSEO
# ---------------------------------------------------------------------------
# El parseo (formatos Android/iOS, multilínea, adjuntos) vive en
# mensajeria/whatsapp_formato.py para compartirlo con el importador y los workers.


def parse_chat(ruta: str, dia_primero: bool | None = None) -> pd.DataFrame:
    """Lee el .txt exportado y devuelve un DataFrame con fecha, autor y mensaje.
    Usa el parser común de mensajeria/whatsapp_formato.py (el mismo que usan el
    importador y el worker de texto). dia_primero=None lo detecta automáticamente."""
    from mensajeria.whatsapp_formato import parsear_archivo
    df = pd.DataFrame(m.a_dict() for m in parsear_archivo(ruta, dia_primero))
    df = df.rename(columns={"texto": "mensaje", "tipo": "tipo_wa"})
    df["fecha_hora"] = pd.to_datetime(df["fecha_hora"])
    return df


# ---------------------------------------------------------------------------
# 2. LIMPIEZA
# ---------------------------------------------------------------------------
MARCAS_MEDIA = {
    "media": ["<multimedia omitido>", "<media omitted>", "imagen omitida", "image omitted",
              "video omitido", "video omitted", "gif omitido", "gif omitted", "documento omitido"],
    "voz": ["audio omitido", "audio omitted", "nota de voz"],
    "sticker": ["sticker omitido", "sticker omitted"],
    "ubicacion": ["ubicación:", "location:"],
    "eliminado": ["se eliminó este mensaje", "eliminaste este mensaje", "this message was deleted"],
}

# Palabras vacías básicas en español e inglés (se completan con las del propio chat)
STOPWORDS = set("""
a al algo ante antes así aun aunque bien cada como con contra cual cuando de del desde donde
dos e el ella ellas ellos en entre era es esa ese eso esta este esto estoy está están fue
ha hay he la las le les lo los me mi mis muy más ni no nos o os para pero por porque que qué
se si sí sin sobre son su sus también te ti tu tus un una uno unos y ya yo vos usted ustedes
jaja jajaja jajajaja xd ok q pq tmb voy va ver bueno pues eh ah oh
the and to of i you it is a in that for on my me be are was this with so just do not have
""".split())


# Clasificación del parser común → categorías de este análisis
TIPO_WA = {"sistema": "sistema", "eliminado": "eliminado", "omitido": "media",
           "imagen": "media", "video": "media", "documento": "media", "voz": "voz", "audio": "voz"}


def tipo_mensaje(row) -> str:
    if row.get("tipo_wa") in TIPO_WA:
        return TIPO_WA[row["tipo_wa"]]
    if pd.isna(row["autor"]):
        return "sistema"
    texto = row["mensaje"].lower()
    for tipo, marcas in MARCAS_MEDIA.items():
        if any(m in texto for m in marcas):
            return tipo
    return "texto"


def clean_messages(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["tipo"] = df.apply(tipo_mensaje, axis=1)
    df["emojis"] = df["mensaje"].apply(lambda t: [e["emoji"] for e in emoji.emoji_list(t)])
    df["urls"] = df["mensaje"].str.findall(r"https?://\S+")
    # Texto sin emojis ni enlaces, en minúscula, para análisis de palabras
    df["texto_limpio"] = (
        df["mensaje"]
        .str.replace(r"https?://\S+", " ", regex=True)
        .apply(lambda t: emoji.replace_emoji(t, replace=" "))
        .str.lower()
        .str.replace(r"[^\wáéíóúüñ\s]", " ", regex=True)
    )
    df.loc[df["tipo"] != "texto", "texto_limpio"] = ""
    df["palabras"] = df["texto_limpio"].str.split().str.len().fillna(0).astype(int)
    df["dia_semana"] = df["fecha_hora"].dt.day_of_week  # 0 = lunes
    df["hora"] = df["fecha_hora"].dt.hour
    return df


# ---------------------------------------------------------------------------
# 3. ANÁLISIS
# ---------------------------------------------------------------------------
def analyze(df: pd.DataFrame, con_sentimiento: bool = False) -> dict:
    usuarios = df[df["tipo"] != "sistema"]
    textos = usuarios[usuarios["tipo"] == "texto"]

    por_autor = usuarios.groupby("autor").agg(
        mensajes=("mensaje", "size"),
        palabras_promedio=("palabras", "mean"),
        multimedia=("tipo", lambda s: (s != "texto").sum()),
    ).sort_values("mensajes", ascending=False)
    por_autor["porcentaje"] = (100 * por_autor["mensajes"] / por_autor["mensajes"].sum()).round(1)

    linea_tiempo = usuarios.set_index("fecha_hora").resample("D").size()

    calor = (
        usuarios.pivot_table(index="dia_semana", columns="hora", values="mensaje",
                             aggfunc="size", fill_value=0)
        .reindex(index=range(7), columns=range(24), fill_value=0)
    )

    tokens = [p for t in textos["texto_limpio"] for p in t.split()
              if p not in STOPWORDS and len(p) > 2 and not p.isdigit()]
    palabras = Counter(tokens)

    emojis = Counter(e for lista in usuarios["emojis"] for e in lista)
    dominios = Counter(urlparse(u).netloc.replace("www.", "")
                       for lista in usuarios["urls"] for u in lista)

    # Tiempo de respuesta: minutos entre mensajes consecutivos de autores distintos
    orden = usuarios.sort_values("fecha_hora")
    cambio = orden["autor"] != orden["autor"].shift()
    espera = orden["fecha_hora"].diff().dt.total_seconds() / 60
    respuesta = (orden.assign(espera=espera)[cambio & (espera < 12 * 60)]
                 .groupby("autor")["espera"].median().round(1))

    resultados = {
        "por_autor": por_autor,
        "linea_tiempo": linea_tiempo,
        "calor": calor,
        "palabras": palabras,
        "emojis": emojis,
        "dominios": dominios,
        "respuesta_min": respuesta,
        "sistema": df[df["tipo"] == "sistema"][["fecha_hora", "mensaje"]],
    }

    if con_sentimiento:
        # Ojo: VADER está pensado para inglés; en español da resultados poco fiables.
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        sia = SentimentIntensityAnalyzer()
        textos = textos.assign(sentimiento=textos["mensaje"].apply(
            lambda t: sia.polarity_scores(t)["compound"]))
        resultados["sentimiento"] = textos.groupby("autor")["sentimiento"].mean().round(3)

    return resultados


# ---------------------------------------------------------------------------
# 4. VISUALIZACIÓN
# ---------------------------------------------------------------------------
DIAS = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]


def visualize(r: dict, df: pd.DataFrame) -> None:
    SALIDA.mkdir(exist_ok=True)
    sns.set_theme(style="whitegrid")

    # Mensajes por persona
    top = r["por_autor"].head(15)
    fig, ax = plt.subplots(figsize=(9, max(3, 0.45 * len(top))))
    sns.barplot(x=top["mensajes"], y=top.index, ax=ax, color="#3b82f6")
    ax.set(title="Mensajes por persona", xlabel="Mensajes", ylabel="")
    fig.tight_layout(); fig.savefig(SALIDA / "mensajes_por_persona.png", dpi=150); plt.close(fig)

    # Línea de tiempo (promedio móvil de 7 días para suavizar)
    fig, ax = plt.subplots(figsize=(11, 4))
    r["linea_tiempo"].plot(ax=ax, alpha=0.35, label="Por día")
    r["linea_tiempo"].rolling(7, min_periods=1).mean().plot(ax=ax, label="Promedio 7 días")
    ax.set(title="Actividad en el tiempo", xlabel="", ylabel="Mensajes"); ax.legend()
    fig.tight_layout(); fig.savefig(SALIDA / "linea_tiempo.png", dpi=150); plt.close(fig)

    # Mapa de calor día x hora
    fig, ax = plt.subplots(figsize=(12, 4))
    sns.heatmap(r["calor"], cmap="Blues", ax=ax, yticklabels=DIAS, cbar_kws={"label": "Mensajes"})
    ax.set(title="¿Cuándo se escribe más?", xlabel="Hora del día", ylabel="")
    fig.tight_layout(); fig.savefig(SALIDA / "mapa_calor.png", dpi=150); plt.close(fig)

    # Nube de palabras
    if r["palabras"]:
        nube = WordCloud(width=1200, height=600, background_color="white",
                         colormap="viridis").generate_from_frequencies(r["palabras"])
        nube.to_file(str(SALIDA / "nube_palabras.png"))

    # Palabras más usadas
    comunes = r["palabras"].most_common(20)
    if comunes:
        palabras, conteos = zip(*comunes)
        fig, ax = plt.subplots(figsize=(8, 6))
        sns.barplot(x=list(conteos), y=list(palabras), ax=ax, color="#10b981")
        ax.set(title="20 palabras más usadas", xlabel="Veces", ylabel="")
        fig.tight_layout(); fig.savefig(SALIDA / "top_palabras.png", dpi=150); plt.close(fig)

    # CSV con los datos limpios
    (df.drop(columns=["linea_cruda"])
       .assign(emojis=df["emojis"].str.join(""), urls=df["urls"].str.join(" "))
       .to_csv(SALIDA / "chat_limpio.csv", index=False, encoding="utf-8-sig"))


def imprimir_resumen(r: dict) -> None:
    print("\n=== Mensajes por persona ===")
    print(r["por_autor"].round(1).to_string())
    print("\n=== Top 10 emojis ===")
    print("  ".join(f"{e} {n}" for e, n in r["emojis"].most_common(10)) or "(ninguno)")
    print("\n=== Top 10 palabras ===")
    print(", ".join(f"{p} ({n})" for p, n in r["palabras"].most_common(10)))
    print("\n=== Sitios más compartidos ===")
    print(", ".join(f"{d} ({n})" for d, n in r["dominios"].most_common(5)) or "(ninguno)")
    print("\n=== Tiempo de respuesta mediano (min) ===")
    print(r["respuesta_min"].to_string())
    if "sentimiento" in r:
        print("\n=== Sentimiento promedio (-1 a 1) ===")
        print(r["sentimiento"].to_string())
    print(f"\nGráficos y CSV guardados en ./{SALIDA}/")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("Uso: python AnalyzeWhatsApp.py chat.txt [--sentimiento]")
    df = clean_messages(parse_chat(sys.argv[1]))
    resultados = analyze(df, con_sentimiento="--sentimiento" in sys.argv)
    imprimir_resumen(resultados)
    visualize(resultados, df)