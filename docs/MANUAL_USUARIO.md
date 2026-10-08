# Manual de usuario

Cómo usar la plataforma una vez que está encendida. Para instalarla, ver el [README](../README.md).

Se puede usar de dos formas:

| | Para qué |
|---|---|
| **[El dashboard](#1-el-dashboard)** | Una página web para subir casos y mirar todo |
| **[El cliente](#2-el-cliente-terminal)** | Un programa de terminal para mandar muchos casos de una vez y medir tiempos |

### Contenido

1. [El dashboard](#1-el-dashboard)
   - [Tarjetas de arriba](#11-tarjetas-de-arriba) · [Colas](#12-colas) · [Subir un caso](#13-subir-un-caso) · [Workers activos](#14-workers-activos)
   - [Casos](#15-casos) · [Detalle de un caso](#16-detalle-de-un-caso) · [Variedad de archivos](#17-variedad-de-archivos-recibidos) · [Trazabilidad](#18-trazabilidad)
2. [El cliente (terminal)](#2-el-cliente-terminal)
3. [Armar el dataset de prueba](#3-armar-el-dataset-de-prueba)
4. [Problemas frecuentes](#4-problemas-frecuentes)

---

## 1. El dashboard

Abrilo en el navegador: `http://<IP del coordinador>:8000` (en la computadora del coordinador, `http://localhost:8000`). Se actualiza solo cada 3 segundos.

Estas son sus partes, de arriba hacia abajo.

### 1.1 Tarjetas de arriba

Totales del sistema: casos, casos en proceso, casos completados, workers conectados y **archivos procesados**. Un archivo cuenta como procesado cuando terminaron todas sus tareas.

### 1.2 Colas

Una tarjeta por cola (**video**, **audio**, **ligera**) que dice **cuántas tareas están esperando** y cuántos workers las atienden.

> [!TIP]
> Si hay tareas esperando y dice *"nadie atiende este pool"*, falta encender un worker.

### 1.3 Subir un caso

En **Nuevo caso**:

1. Ponele un nombre (opcional).
2. Elegí la **prioridad**: 1 es baja y 10 es alta. Un caso con más prioridad pasa adelante de los que ya estaban esperando.
3. Arrastrá los archivos al recuadro punteado, o hacé clic para elegirlos. Podés mezclar videos, audios e imágenes. La **×** quita un archivo de la lista.
4. Tocá **Enviar caso**. Se ve el avance de la subida y, al final, el número del caso creado.

> [!NOTE]
> Si mandás un tipo de archivo que no está soportado (por ejemplo, un PDF desde el cliente), ese archivo queda marcado como **"formato no soportado"** y el resto del caso se procesa igual.

### 1.4 Workers activos

Arriba de la tabla hay un resumen, por ejemplo: *"4 workers conectados en 4 máquinas distintas"*.

| Columna | Qué muestra |
|---|---|
| **IP** | Desde qué dirección llega cada worker. Si dice *"misma máquina que el coordinador"*, corre en la misma computadora. |
| **Máquina** | Nombre de la computadora, procesador, núcleos, RAM y sistema operativo |
| **Estado** | `libre`, `ocupado`, `saturado` (CPU muy alta: no toma tareas nuevas por un rato) o `desconectado` (no da señales hace más de 15 s) |
| **CPU / Memoria** | Cuánto está usando ahora |
| **Tareas** | Cuántas está haciendo en este momento |
| **Procesadas** | Cuántas terminó en total (sirve para ver cómo se repartió el trabajo) |
| **Versión** | `v5` si está actualizado; `desactualizado` si hay que reiniciarlo |

Los desconectados quedan en gris al final, y se pueden sacar de la lista con **Quitar**.

### 1.5 Casos

Cada fila es un caso, con su estado, una barra de avance, el tamaño total de sus archivos y cuántas tareas hay en espera, en ejecución, completadas o fallidas.

![Tabla de casos con los botones Pausar, Cancelar y Eliminar](img/tabla_casos.png)

| Estado | Significa |
|---|---|
| `en cola` / `en proceso` | Todavía se está trabajando |
| `reintentando` | Alguna tarea falló por la red y se va a volver a intentar |
| `en pausa` | Lo pausaste: lo que estaba corriendo termina, lo demás espera |
| `completado` | Todo salió bien |
| `parcialmente completado` | Terminó, pero alguna tarea falló |
| `fallido` | No salió bien ninguna tarea |
| `cancelado` | Se canceló |

**Botones**

| Botón | Cuándo aparece | Qué hace |
|---|---|---|
| *Clic en la fila* | Siempre | Abre el detalle del caso |
| **Pausar** | Si no terminó | Las tareas que ya están corriendo terminan, pero las que esperan **no arrancan**, y los workers quedan libres para otros casos |
| **Reanudar** | Si está en pausa | Lo que quedó pendiente vuelve a la cola y el caso sigue donde quedó |
| **Cancelar** | Si no terminó, aunque esté en pausa | Las tareas que faltan ya no se hacen y el caso queda cerrado |
| **Eliminar** | Siempre | Borra el caso y todos sus archivos |

> [!WARNING]
> **Eliminar** no se puede deshacer: se borran los originales y los resultados del caso.

### 1.6 Detalle de un caso

Arriba aparece el **resumen en una línea**, por ejemplo: *"De 15 archivos — 6 miniaturas generadas, 4 audios convertidos…; 1 fallido por formato no soportado"*.

Debajo está la tabla con cada tarea: qué operación es, su estado (con % mientras avanza), qué worker la hizo y cuánto tardó. En la columna **Resultado**:

- **Descargar:** baja el archivo resultante.
- **Ver error:** pasá el mouse para leer por qué falló.
- Una nota pequeña con información extra: en qué segundo se tomó la portada, el álbum encontrado, o el tamaño antes y después de convertir.

| Botón | Qué hace |
|---|---|
| **Ver reporte consolidado** | Abre el reporte completo del caso, que se puede imprimir o guardar como PDF |
| **Descargar JSON** | El mismo reporte, en formato de datos |

### 1.7 Variedad de archivos recibidos

Gráficos de todos los archivos que llegaron al sistema:

- **Archivos por tipo:** cuántos videos, audios e imágenes hay, y cuánto pesan.
- **Formatos:** cuántos de cada extensión (MP4, MKV, MP3, WAV, PNG…).
- **Distribución de tamaños:** cuántos archivos hay en cada rango, desde menos de 1 MB hasta más de 600 MB. Debajo dice cuánto pesan los archivos de más de 200 MB.

**Ver datos en tabla** muestra los mismos números en una tabla.

![Gráficos de variedad de archivos: por tipo, por formato y por tamaño](img/variedad_archivos.png)

### 1.8 Trazabilidad

Muestra **qué worker hizo cada cosa**:

- **Mapa de flujo:** caso → archivo → tarea → worker, con un color por worker. Al pasar el mouse sobre un elemento se resalta su recorrido.
- **Línea de tiempo:** una fila por worker y una barra por tarea. **Las barras que se superponen corrieron al mismo tiempo**: ahí se ve el paralelismo.
- **Resumen:** cuánto tardó todo, cuánto habría tardado haciendo una cosa por vez, y la **aceleración** lograda (speedup).

![Mapa de flujo de un caso: archivos, sub-tareas y el worker que hizo cada una](img/trazabilidad_mapa.png)

---

## 2. El cliente (terminal)

Se corre desde la carpeta del proyecto, con el entorno activado:

```bash
source .venv/bin/activate
```

Si el coordinador está en otra computadora, primero:

```bash
export COORDINATOR_URL=http://192.168.1.100:8000
```

| Para… | Comando |
|---|---|
| Mandar una carpeta como un caso | `python cliente/cliente.py enviar ./dataset_real/caso_022_heterogeneo` |
| Mandar todo el dataset (cada carpeta es un caso) | `python cliente/cliente.py carga ./dataset_real --concurrentes 4 --metricas pruebas/carga.csv` |
| Mandar solo algunos casos | Agregar `--filtro heterogeneo` (los que tengan esa palabra en el nombre) o `--max-casos 5` |
| Armar casos automáticamente por evento | `python cliente/cliente.py auto ./dataset_real --por evento` (también `sesion`, `usuario` o `lote`) |
| Ver cómo va un caso | `python cliente/cliente.py estado case-xxxxxxxx` |
| Bajar todos los resultados de un caso | `python cliente/cliente.py resultados case-xxxxxxxx --salida ./resultados_descargados` |
| Pausar un caso | `python cliente/cliente.py pausar case-xxxxxxxx` |
| Reanudar un caso | `python cliente/cliente.py reanudar case-xxxxxxxx` |
| Cancelar un caso | `python cliente/cliente.py cancelar case-xxxxxxxx` |
| Ver el estado general | `python cliente/cliente.py resumen` |

Con `--metricas`, el cliente **espera a que termine todo** y muestra un resumen como este *(números de ejemplo)*:

```text
MÉTRICAS DE LA PRUEBA
  Casos: 34  {'completed': 30, 'partially_completed': 4}
  Archivos: 480 · Sub-tareas: 560
  Tiempo total: 412.3 s
  Rendimiento: 69.8 archivos/min
  Tiempo secuencial estimado: 1103.5 s → speedup global 2.68×
  Reparto por worker:
    worker-1    210 sub-tareas (37.5%) · ...
```

Los datos quedan guardados en un `.csv` y un `.json` dentro de `pruebas/`, para el informe.

---

## 3. Armar el dataset de prueba

### Con archivos reales (recomendado; necesita internet)

```bash
python scripts/descargar_dataset.py
```

- Baja videos, canciones y fotos reales de Wikimedia Commons, todos de uso libre.
- A partir de ellos arma **~480 archivos** en casos homogéneos (todos del mismo tipo) y heterogéneos (mezclados), con tamaños livianos, medianos, pesados y **10 archivos de 400–600 MB**.
- Ocupa unos 9 GB y queda en `dataset_real/`.
- Los créditos de cada original quedan en `dataset_real/CREDITOS.md`.

**Opciones útiles:**

| Opción | Para qué |
|---|---|
| `--limpiar` | Volver a generar todo (reutiliza lo ya descargado) |
| `--pesados-desde 22` | Conservar los casos 1 a 21 y rehacer del 22 en adelante como casos heterogéneos de solo archivos pesados |
| `--muy-pesados 0` | No generar los archivos de 400–600 MB |
| `--ligero` | Archivos chicos, para probar rápido |

### Sin internet (archivos sintéticos: patrones de colores y tonos)

```bash
python scripts/generar_dataset.py
```

---

## 4. Problemas frecuentes

| Qué pasa | Qué hacer |
|---|---|
| Todas las tareas fallan con `Error opening input file uploads/...` | El worker es una versión vieja. Reinicialo y, si usa Docker, reconstruí la imagen (`docker build -t worker .`). |
| Un worker aparece conectado pero no lo ves en ninguna terminal | Está corriendo en segundo plano. Buscalo con `docker ps` y detenelo con `docker stop <nombre>`. |
| Hay tareas esperando y nadie las toma | No hay ningún worker encendido para esa cola. Encendé uno. |
| Un worker queda **saturado** mucho rato | Esa computadora está ocupada con otra cosa. Se libera sola cuando baja el uso de CPU. |
| `permission denied ... docker.sock` | Usá `sudo` delante de `docker`. |
| Al mandar casos grandes da `400 Bad Request` | El coordinador es una versión vieja: reinicialo. |
| Las horas se ven corridas | Reiniciá el coordinador. |
