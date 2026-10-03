# Manual de usuario

Este manual explica cómo usar la plataforma una vez desplegada (ver el [README](../README.md) para la instalación). Hay dos formas de usarla: el **dashboard web** y el **cliente de línea de comandos**.

---

## 1. Dashboard web

Se abre en `http://<IP del coordinador>:8000` desde cualquier navegador de la red. Se actualiza solo cada 3 segundos.

### 1.1 Tarjetas generales

En la parte superior se ven los casos totales, en proceso y completados, los workers activos (los que enviaron heartbeat en los últimos 15 s) y los **archivos procesados**. Un archivo cuenta como procesado cuando terminaron todas sus sub-tareas. Debajo se indica cuántos terminaron sin errores, cuántos con fallas y el total de sub-tareas.

### 1.2 Colas por pool

Una tarjeta por pool (`video`, `audio`, `ligera`) muestra cuántas sub-tareas **esperan** en RabbitMQ y cuántos workers lo atienden. Si hay sub-tareas esperando y ningún worker atiende ese pool, aparece **nadie atiende este pool**: hay que levantar un worker genérico o uno de ese pool.

### 1.3 Enviar un caso

En **Nuevo caso**:

1. Escriba un nombre para el caso (opcional).
2. Elija la **prioridad**: 1 = baja, 10 = alta. Las sub-tareas de un caso con más prioridad se procesan antes que las que ya esperaban.
3. Arrastre los archivos a la zona punteada o haga clic para elegirlos. Puede mezclar videos, audios e imágenes (caso heterogéneo). Para quitar un archivo de la lista, use la **×**.
4. Pulse **Enviar caso**. Se muestra el avance de la subida y, al terminar, el ID del caso creado.

> El selector solo ofrece los formatos soportados. Si envía otro tipo de archivo (por ejemplo, con el cliente), queda registrado como **fallido por formato no soportado** y el resto del caso se procesa normalmente.

### 1.4 Workers activos

Para cada worker se muestra:

| Columna | Significado |
|---|---|
| Estado | **libre**, **ocupado**, **saturado** (CPU al límite: no toma tareas nuevas hasta que baje) o **desconectado** (sin heartbeat hace más de 15 s) |
| Pools | Qué colas atiende; ×N indica cuántas sub-tareas procesa en paralelo |
| CPU / Memoria | Último uso reportado |
| Tareas | Sub-tareas que está procesando ahora |
| Procesadas | Sub-tareas que terminó en total (muestra el reparto de carga) |
| Versión | **v4** si corre la versión actual; **desactualizado** si hay que reiniciarlo o reconstruir su imagen |

Los workers desconectados quedan en gris al final y se pueden quitar de la lista con **Quitar**.

### 1.5 Casos de procesamiento

Cada fila muestra el estado agregado del caso, una barra de progreso y el desglose de sub-tareas: en espera, en ejecución, completadas, fallidas y canceladas.

| Estado | Significado |
|---|---|
| en cola | se está registrando |
| en proceso | hay sub-tareas sin terminar |
| reintentando | hay sub-tareas esperando un reintento tras un error de red |
| completado | todo salió bien |
| parcialmente completado | terminó con al menos una sub-tarea fallida |
| fallido | ninguna sub-tarea tuvo éxito |
| cancelado | se canceló |

Acciones:

- **Clic en la fila:** abre el detalle del caso (ver 1.6) y enfoca la trazabilidad en él.
- **Cancelar** (solo casos sin terminar): las sub-tareas pendientes ya no se procesan. Las que estaban en ejecución terminan, pero su resultado se descarta.
- **Eliminar:** borra el caso, sus sub-tareas, sus archivos originales y sus resultados. No se puede deshacer.

### 1.6 Detalle del caso

Muestra el **resumen agregado** (por ejemplo: "De 15 archivos (6 audios, 6 imágenes, 3 videos) — 6 miniaturas generadas, 4 audios convertidos, …; 1 fallido (1 por formato no soportado)") y la tabla de sub-tareas con su operación, pool, estado con % de avance, worker, duración y resultado:

- **Descargar:** baja el archivo resultante.
- **Ver error:** pase el mouse para leer el motivo del fallo.
- "N reintento(s)" indica reintentos y "redistribuida" indica que la sub-tarea pasó a otro worker porque el anterior se cayó.

Botones:

- **Ver reporte consolidado:** abre el reporte del caso en una pestaña nueva, con el botón **Imprimir / PDF**.
- **Descargar JSON:** el mismo reporte en formato JSON.

### 1.7 Variedad de archivos recibidos

Resume todos los archivos enviados al sistema, para mostrar la variedad del dataset:

- **Resumen:** cantidad de archivos, volumen total, formatos distintos y tamaño mínimo, mediano y máximo.
- **Archivos por tipo:** cantidad y volumen de video, audio, imagen y otros.
- **Formatos:** cuántos archivos hay de cada extensión, con el color de su tipo.
- **Distribución de tamaños:** cuántos archivos caen en cada rango (menos de 100 KB, 100 KB a 1 MB, 1 a 10 MB, 10 a 50 MB, más de 50 MB), separados por tipo.

Al pasar el mouse sobre una barra se ve el detalle, y **Ver datos en tabla** muestra los mismos números en una tabla.

### 1.8 Trazabilidad

- **Selector de caso:** últimos 5 casos o uno en particular.
- **Resumen:** workers que participaron, máximo de sub-tareas en paralelo, tiempo real, tiempo si fuera secuencial y **aceleración (speedup)**.
- **Mapa de flujo:** caso → archivo → sub-tarea → worker, con un color por worker. Al pasar el mouse sobre un nodo se resalta su recorrido. Las líneas animadas son sub-tareas en ejecución y las punteadas, sub-tareas en cola.
- **Línea de tiempo por worker:** una fila por worker y una barra por sub-tarea. Las barras superpuestas en vertical son sub-tareas que corrieron al mismo tiempo. Al mover el mouse se ve qué se ejecutaba en cada instante.

---

## 2. Cliente de línea de comandos

Desde la carpeta del proyecto, con el entorno virtual activo. Si el coordinador está en otra máquina:

```bash
export COORDINATOR_URL=http://192.168.1.100:8000
```

| Comando | Qué hace |
|---|---|
| `python cliente/cliente.py enviar <carpeta> [--prioridad N] [--nombre X]` | Envía todos los archivos de la carpeta como un caso, con su `metadata.json` si existe |
| `python cliente/cliente.py carga <dataset> --concurrentes 5 --esperar` | Envía cada sub-carpeta como un caso, 5 envíos a la vez, y espera a que terminen |
| `python cliente/cliente.py auto <dataset> --por evento` | **Generación automática:** agrupa los archivos del catálogo por `evento`, `sesion`, `usuario`, `lote` o `carpeta` y envía un caso por grupo |
| `... --metricas pruebas/archivo.csv` | Con `carga` o `auto`: al terminar guarda métricas por caso (CSV) y un resumen (JSON) |
| `... --prioridad-aleatoria` / `--max-casos N` | Prioridad al azar por caso / enviar solo los primeros N |
| `python cliente/cliente.py estado <case-id>` | Estado del caso, sub-tareas y resumen |
| `python cliente/cliente.py esperar <case-id> ...` | Espera a que terminen uno o varios casos |
| `python cliente/cliente.py resultados <case-id> --salida ./carpeta` | Descarga el reporte consolidado y todos los resultados del caso |
| `python cliente/cliente.py cancelar <case-id>` | Cancela un caso |
| `python cliente/cliente.py resumen` | Estado del sistema: casos, sub-tareas, colas y workers |

Ejemplo de salida de `--metricas`:

```
MÉTRICAS DE LA PRUEBA
Casos: 34  {'completed': 30, 'partially_completed': 4}
Archivos: 480 · Sub-tareas: 560
Tiempo total: 412.3 s (envío 18.2 s)
Rendimiento: 69.8 archivos/min
Tiempo secuencial estimado: 1103.5 s → speedup global 2.68×
Reparto por worker:
  worker-1    210 sub-tareas (37.5%) · ocupado 402.1 s · fallidas 2
  ...
```

*(Los números son ilustrativos: dependen del equipo y la red.)*

---

## 3. Dataset de prueba

**Con archivos reales (recomendado):**

```bash
python scripts/descargar_dataset.py                 # ~480 archivos en ./dataset_real
python scripts/descargar_dataset.py --escala 0.2    # versión chica para probar
```

Ocupa unos 9 GB e incluye 10 archivos de 400–600 MB (7 videos Full HD largos y 3 WAV de ~45 min), más un caso con 4 canciones y uno de esos videos. Con `--muy-pesados 0` no se generan; con `--ligero` todos los archivos son chicos, para probar rápido.

Busca en Wikimedia Commons videos de conciertos, festivales y desfiles, audios de piano, orquesta y coros, e imágenes de graduaciones, campus y escenarios, todos con licencia libre. A partir de esos originales recorta fragmentos livianos, medianos y pesados y los exporta a todos los formatos soportados. Los metadatos de cada archivo incluyen el título, autor, licencia y enlace reales, y `CREDITOS.md` lista la atribución que piden las licencias. Los originales quedan en `dataset_real/_originales/` y no se vuelven a descargar si se corre el script de nuevo con `--limpiar`.

**Sintético, sin internet:**

```bash
python scripts/generar_dataset.py                 # ~480 archivos en ./dataset_prueba
```

Los dos generan la misma estructura de casos: homogéneos (solo audio a convertir, solo mp3, solo video, solo imágenes) y heterogéneos (audio + video + imágenes y, en algunos, un archivo no soportado o corrupto). Cada caso trae un `metadata.json`. En la raíz quedan `catalogo.json` (todos los archivos con sus metadatos, usado por `cliente.py auto`) y `composicion.json` (cantidades por tipo y tamaño, y volumen total).

---

## 4. Problemas frecuentes

| Síntoma | Causa / solución |
|---|---|
| Todas las sub-tareas fallan con `Error opening input file uploads/...` | El worker corre una versión vieja. Reinícielo o reconstruya su imagen (`docker build -t worker .`). La columna **Versión** lo indica. |
| Un worker aparece conectado pero no se ve en ninguna terminal | Se lanzó con `docker run -d` (segundo plano). Véalo con `docker ps` y deténgalo con `docker stop <nombre>`. |
| Sub-tareas esperando en un pool y nadie las toma | Ningún worker atiende ese pool. Levante uno con `WORKER_POOLS` que lo incluya, o uno genérico. |
| Un worker queda **saturado** mucho tiempo | La PC está ocupada con otra cosa o el umbral es bajo. Ajuste `CPU_HIGH` y `CPU_LOW`. |
| `permission denied ... docker.sock` | El usuario no está en el grupo `docker`: use `sudo` o ejecute `sudo usermod -aG docker $USER` y vuelva a iniciar sesión. |
| Las horas se ven corridas | Reinicie el coordinador: convierte las horas de la BD (UTC) a la hora local. |
