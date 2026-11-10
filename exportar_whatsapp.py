"""
Exporta automáticamente un chat de WhatsApp desde un celular Android y lo copia a la computadora.
Después lo importa como caso de la plataforma (mensajeria/importar.py) o lo analiza
con AnalyzeWhatsApp.py.

Requisitos (una sola vez):
  1. En el celular: Ajustes > Acerca del teléfono > tocar 7 veces "Número de compilación"
     para activar las Opciones de desarrollador; luego activar "Depuración USB".
  2. En la computadora: instalar adb (Android platform-tools) y
         pip install uiautomator2
  3. Conectar el celular por USB y aceptar el aviso "¿Permitir depuración USB?".
     Comprobar con:  adb devices

Uso:
  python exportar_whatsapp.py "Nombre del chat"                   (con archivos → .zip)
  python exportar_whatsapp.py "Nombre del chat" --sin-archivos    (solo el .txt)
  python exportar_whatsapp.py "Nombre del chat" --importar        (exporta e importa como caso)
  python exportar_whatsapp.py "Nombre del chat" --importar --declaro
  python exportar_whatsapp.py "Nombre del chat" --analizar        (exporta y analiza)

Antes de exportar con archivos, abre el chat y descarga las fotos/audios que tengan
el ícono de descarga: WhatsApp solo exporta lo que está guardado en el celular.

Cómo funciona:
  El script toca los mismos botones que tocarías a mano:
  ⋮ > Más > Exportar chat > Incluir archivos | Sin archivos > (app de destino) > Guardar
  El archivo queda en la carpeta Descargas del celular y se copia a ./exportados/.
  Si algún botón tiene otro nombre en tu celular, el script se detiene y te pide que
  toques ese paso a mano; en cuanto el archivo aparece en Descargas, sigue solo.
"""

import subprocess
import sys
import time
import zipfile
from pathlib import Path

import uiautomator2 as u2

PAQUETE = "com.whatsapp"          # usa "com.whatsapp.w4b" para WhatsApp Business
CARPETA_CEL = "/sdcard/Download"
CARPETA_PC = Path("exportados")
ESPERA_MANUAL = 180                # segundos que espera si hay que tocar algo a mano

# Textos posibles de cada botón (español e inglés). Agrega aquí los de tu celular si cambian.
BOTON_BUSCAR = ["Buscar", "Search", "Preguntar a Meta AI o buscar", "Ask Meta AI or Search"]
BOTON_MENU = ["Más opciones", "More options"]
BOTON_MAS = ["Más", "More"]
BOTON_EXPORTAR = ["Exportar chat", "Export chat"]
BOTON_SIN_ARCHIVOS = ["Sin archivos", "Without media", "Sin multimedia"]
BOTON_CON_ARCHIVOS = ["Incluir archivos", "Include media", "Adjuntar archivos", "Incluir multimedia"]
# App del menú "Compartir" que guarda el archivo en Descargas (Files de Google por defecto)
DESTINO = ["Archivos", "Files", "Files de Google", "Files by Google", "Guardar en Archivos"]
BOTON_GUARDAR = ["Guardar", "Save", "GUARDAR", "SAVE"]


def tocar(d, textos, timeout=5) -> bool:
    """Busca un elemento por texto o por descripción y lo toca. Devuelve True si lo encontró."""
    fin = time.time() + timeout
    while time.time() < fin:
        for t in textos:
            for sel in (d(text=t), d(description=t), d(textContains=t)):
                if sel.exists:
                    sel.click()
                    time.sleep(0.8)
                    return True
        time.sleep(0.4)
    return False


def archivos_en_descargas(d) -> set[str]:
    salida = d.shell(f"ls -1 {CARPETA_CEL}").output
    return {l.strip() for l in salida.splitlines()
            if "whatsapp" in l.lower() and l.strip().endswith((".txt", ".zip"))}


def tamano(d, archivo: str) -> int:
    salida = d.shell(f'stat -c %s "{CARPETA_CEL}/{archivo}"').output.strip()
    return int(salida) if salida.isdigit() else -1


def esperar_escritura(d, archivo: str, estable: int = 3, timeout: int = 600) -> None:
    """Espera a que el tamaño del archivo deje de cambiar: con archivos incluidos el
    .zip puede pesar cientos de MB y tardar en escribirse."""
    anterior, iguales, fin = -1, 0, time.time() + timeout
    while time.time() < fin and iguales < estable:
        actual = tamano(d, archivo)
        iguales = iguales + 1 if actual == anterior and actual > 0 else 0
        anterior = actual
        time.sleep(1)
    print(f"Archivo listo en el celular: {anterior / 2**20:.1f} MB")


def abrir_chat(d, nombre: str) -> None:
    d.app_start(PAQUETE, stop=True)
    d(packageName=PAQUETE).wait(timeout=10)
    time.sleep(2)
    if not tocar(d, BOTON_BUSCAR) and not d(resourceId=f"{PAQUETE}:id/menuitem_search").click_exists(3):
        raise RuntimeError("No encontré el botón de búsqueda")
    time.sleep(0.8)
    d.send_keys(nombre, clear=True)
    time.sleep(1.5)
    # Toca el primer resultado que coincida con el nombre (sin tocar la caja de texto)
    resultado = d(resourceId=f"{PAQUETE}:id/conversations_row_contact_name", text=nombre)
    if not resultado.exists:
        resultado = d(className="android.widget.TextView", text=nombre)
    if not resultado.wait(timeout=5):
        raise RuntimeError(f'No encontré el chat "{nombre}" en los resultados')
    resultado.click()
    time.sleep(1.5)


def exportar(d, nombre: str, con_archivos: bool = True) -> Path:
    antes = archivos_en_descargas(d)
    try:
        abrir_chat(d, nombre)
        pasos = [("⋮ Más opciones", BOTON_MENU), ("Más", BOTON_MAS),
                 ("Exportar chat", BOTON_EXPORTAR),
                 ("Incluir archivos", BOTON_CON_ARCHIVOS) if con_archivos
                 else ("Sin archivos", BOTON_SIN_ARCHIVOS)]
        for etiqueta, textos in pasos:
            if not tocar(d, textos, timeout=8):
                raise RuntimeError(f'No encontré "{etiqueta}"')
        print("Preparando el archivo...")
        time.sleep(10 if con_archivos else 3)  # WhatsApp tarda en generar el archivo
        if not tocar(d, DESTINO, timeout=15):
            raise RuntimeError("No encontré la app de destino en el menú Compartir")
        tocar(d, BOTON_GUARDAR, timeout=10)
    except RuntimeError as e:
        print(f"\n⚠ {e}.")
        print("Termina ese paso a mano en el celular y guarda el archivo en Descargas.")
        print(f"Te espero hasta {ESPERA_MANUAL} segundos...")

    # Espera a que aparezca el archivo nuevo en Descargas
    fin = time.time() + ESPERA_MANUAL
    while time.time() < fin:
        nuevos = archivos_en_descargas(d) - antes
        if nuevos:
            break
        time.sleep(2)
    else:
        sys.exit("No apareció ningún archivo nuevo en Descargas. Intenta de nuevo.")

    nombre_archivo = sorted(nuevos)[-1]
    esperar_escritura(d, nombre_archivo)
    CARPETA_PC.mkdir(exist_ok=True)
    destino = CARPETA_PC / nombre_archivo
    d.pull(f"{CARPETA_CEL}/{nombre_archivo}", str(destino))
    print(f"✔ Chat copiado a {destino}")

    return destino


def txt_para_analizar(archivo: Path) -> Path:
    """AnalyzeWhatsApp.py necesita el .txt: si se exportó un .zip, lo extrae."""
    if archivo.suffix.lower() != ".zip":
        return archivo
    carpeta = CARPETA_PC / archivo.stem
    with zipfile.ZipFile(archivo) as z:
        txt = next(n for n in z.namelist() if n.endswith(".txt") and "/" not in n and ".." not in n)
        z.extract(txt, carpeta)
    return carpeta / txt


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if not args:
        sys.exit('Uso: python exportar_whatsapp.py "Nombre del chat" '
                 '[--sin-archivos] [--importar [--declaro]] [--analizar]')
    chat = args[0]
    dispositivo = u2.connect()  # usa el celular conectado por USB
    print(f"Conectado a {dispositivo.info.get('productName', 'celular')}. Exportando '{chat}'...")
    archivo = exportar(dispositivo, chat, con_archivos="--sin-archivos" not in sys.argv)

    aqui = Path(__file__).resolve().parent
    if "--importar" in sys.argv:
        cmd = [sys.executable, str(aqui / "mensajeria" / "importar.py"), str(archivo), "--fuente", "telefono"]
        if "--declaro" in sys.argv:
            cmd.append("--declaro")
        subprocess.run(cmd, check=True)
    if "--analizar" in sys.argv:
        subprocess.run([sys.executable, str(aqui / "AnalyzeWhatsApp.py"), str(txt_para_analizar(archivo))],
                       check=True)
