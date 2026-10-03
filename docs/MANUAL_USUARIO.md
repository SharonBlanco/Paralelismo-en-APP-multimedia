# Manual de usuario

Cómo usar la plataforma una vez que está encendida (para instalarla, ver el [README](../README.md)).

Se puede usar de dos formas:
- **El dashboard**, una página web: para subir casos y mirar todo.
- **El cliente**, un programa de terminal: para mandar muchos casos de una vez y medir tiempos.

---

## 1. El dashboard

Abrilo en el navegador: `http://<IP del coordinador>:8000` (en la computadora del coordinador, `http://localhost:8000`). Se actualiza solo cada 3 segundos.

Estas son sus partes, de arriba hacia abajo.

### 1.1 Tarjetas de arriba

Totales del sistema: casos, casos en proceso, casos completados, workers conectados y **archivos procesados**. Un archivo cuenta como procesado cuando terminaron todas sus tareas.

### 1.2 Colas

Una tarjeta por cola (`video`, `audio`, `ligera`) que dice **cuántas tareas están esperando** y cuántos workers las atienden. Si hay tareas esperando y dice *"nadie atiende este pool"*, falta encender un worker.

### 1.3 Subir un caso

En **Nuevo caso**:

1. Ponele un nombre (opcional).
2. Elegí la **prioridad**: 1 es baja y 10 es alta. Un caso con más prioridad pasa adelante de los que ya estaban esperando.
3. Arrastrá los archivos al recuadro punteado, o hacé clic para elegirlos. Podés mezclar videos, audios e imágenes. La **×** quita un archivo de la lista.
4. Tocá **Enviar caso**. Se ve el avance de la subida y, al final, el número del caso creado.

Si mandás un tipo de archivo que no está soportado (por ejemplo, un PDF desde el cliente), ese archivo queda marcado como **"formato no soportado"** y el resto del caso se procesa igual.

### 1.4 Workers activos

Arriba de la tabla hay un resumen, por ejemplo: *"3 workers conectados en 3 máquinas distintas"*.

| Columna | Qué muestra |
|---|---|
| IP | Desde qué dirección llega cada worker. Si dice *"misma máquina que el coordinador"*, corre en la misma computadora. |
| Máquina | Nombre de la computadora, procesador, núcleos, RAM y sistema operativo |
| Estado | **libre**, **ocupado**, **saturado** (CPU muy alta: no toma tareas nuevas por un rato) o **desconectado** (no da señales hace más de 15 s) |
| CPU / Memoria | Cuánto está usando ahora |
| Tareas | Cuántas está haciendo en este momento |
| Procesadas | Cuántas terminó en total (sirve para ver cómo se repartió el trabajo) |
| Versión | **v4** si está actualizado; **desactualizado** si hay que reiniciarlo |

Los desconectados quedan en gris al final, y se pueden sacar de la lista con **Quitar**.

### 1.5 Casos

Cada fila es un caso, con su estado, una barra de avance y cuántas tareas hay en espera, en ejecución, completadas o fallidas.

| Estado | Significa |
|---|---|
| en cola / en proceso | todavía se está trabajando |
| reintentando | alguna tarea falló por la red y se va a volver a intentar |
| **completado** | todo salió bien |
| **parcialmente completado** | terminó, pero alguna tarea falló |
| **fallido** | no salió bien ninguna tarea |
| cancelado | se canceló |

Botones:
- **Clic en la fila:** abre el detalle del caso.
- **Cancelar** (solo si no terminó): las tareas que faltan ya no se hacen.
- **Eliminar:** borra el caso y todos sus archivos. **No se puede deshacer.**

### 1.6 Detalle de un caso

Arriba aparece el **resumen en una línea**, por ejemplo: *"De 15 archivos — 6 miniaturas generadas, 4 audios convertidos…; 1 fallido por formato no soportado"*.

Debajo está la tabla con cada tarea: qué operación es, su estado (con % mientras avanza), qué worker la hizo y cuánto tardó. En la columna **Resultado**:
- **Descargar:** baja el archivo resultante.
- **Ver error:** pasá el mouse para leer por qué falló.
- Una nota pequeña con información extra: en qué segundo se tomó la portada, el álbum encontrado, o el tamaño antes y después de convertir.

Botones:
- **Ver reporte consolidado:** abre el reporte completo del caso, que se puede imprimir o guardar como PDF.
- **Descargar JSON:** el mismo reporte, en formato de datos.

### 1.7 Variedad de archivos recibidos

Gráficos de todos los archivos que llegaron al sistema:
- **Archivos por tipo:** cuántos videos, audios e imágenes hay, y cuánto pesan.
- **Formatos:** cuántos de cada extensión (MP4, MKV, MP3, WAV, PNG…).
- **Distribución de tamaños:** cuántos archivos hay en cada rango, desde menos de 1 MB hasta más de 600 MB. Debajo dice cuánto pesan los archivos de más de 200 MB.

**Ver datos en tabla** muestra los mismos números en una tabla.

### 1.8 Trazabilidad

Muestra **qué worker hizo cada cosa**:
- **Mapa de flujo:** caso → archivo → tarea → worker, con un color por worker. Al pasar el mouse sobre un elemento se resalta su recorrido.
- **Línea de tiempo:** una fila por worker y una barra por tarea. **Las barras que se superponen corrieron al mismo tiempo**: ahí se ve el paralelismo.
- **Resumen:** cuánto tardó todo, cuánto habría tardado haciendo una cosa por vez, y la **aceleración** lograda (speedup).

---

## 2. El cliente (terminal)

Se corre desde la carpeta del proyecto, con el entorno activado (`source .venv/bin/activate`). Si el coordinador está en otra computadora, primero:

```bash
export COORDINATOR_URL=http://192.168.1.100:8000
```

| Para… | Comando |
|---|---|
| Mandar una carpeta como un caso | `python cliente/cliente.py enviar ./dataset_real/caso_022_heterogeneo` |
| Mandar todo el dataset (cada carpeta es un caso) | `python cliente/cliente.py carga ./dataset_real --concurrentes 4 --metricas pruebas/carga.csv` |
| Mandar solo algunos casos | agregar `--filtro heterogeneo` (los que tengan esa palabra en el nombre) o `--max-casos 5` |
| Armar casos automáticamente por evento | `python cliente/cliente.py auto ./dataset_real --por evento` (también `sesion`, `usuario` o `lote`) |
| Ver cómo va un caso | `python cliente/cliente.py estado case-xxxxxxxx` |
| Bajar todos los resultados de un caso | `python cliente/cliente.py resultados case-xxxxxxxx --salida ./resultados_descargados` |
| Cancelar un caso | `python cliente/cliente.py cancelar case-xxxxxxxx` |
| Ver el estado general | `python cliente/cliente.py resumen` |

Con `--metricas`, el cliente **espera a que termine todo** y muestra un resumen como este:

```
MÉTRICAS DE LA PRUEBA
Casos: 34  {'completed': 30, 'partially_completed': 4}
Archivos: 480 · Sub-tareas: 560
Tiempo total: 412.3 s
Rendimiento: 69.8 archivos/min
Tiempo secuencial estimado: 1103.5 s → speedup global 2.68×
Reparto por worker:
  worker-1    210 sub-tareas (37.5%) · ...
```

*(Números de ejemplo.)* Los datos quedan guardados en un `.csv` y un `.json` dentro de `pruebas/`, para el informe.

---

## 3. Armar el dataset de prueba

**Con archivos reales** (recomendado; necesita internet):

```bash
python scripts/descargar_dataset.py
```

- Baja videos, canciones y fotos reales de Wikimedia Commons, todos de uso libre.
- A partir de ellos arma **~480 archivos** en casos homogéneos (todos del mismo tipo) y heterogéneos (mezclados), con tamaños livianos, medianos, pesados y **10 archivos de 400–600 MB**.
- Ocupa unos 9 GB y queda en `dataset_real/`.
- Los créditos de cada original quedan en `dataset_real/CREDITOS.md`.

Opciones útiles:

| Opción | Para qué |
|---|---|
| `--limpiar` | Volver a generar todo (reutiliza lo ya descargado) |
| `--pesados-desde 22` | Conservar los casos 1 a 21 y rehacer del 22 en adelante como casos heterogéneos de solo archivos pesados |
| `--muy-pesados 0` | No generar los archivos de 400–600 MB |
| `--ligero` | Archivos chicos, para probar rápido |

**Sin internet** (archivos sintéticos: patrones de colores y tonos):

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
| Un worker queda **saturado** mucho rato | Esa computadora está ocupada con otra cosa. Se libera solo cuando baja el uso de CPU. |
| `permission denied ... docker.sock` | Usá `sudo` delante de `docker`. |
| Al mandar casos grandes da "400 Bad Request" | El coordinador es una versión vieja: reinicialo. |
| Las horas se ven corridas | Reiniciá el coordinador. |
