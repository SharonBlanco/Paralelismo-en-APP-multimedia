# Informe de pruebas — guía y plantilla

Este documento define las pruebas que evidencian lo que pide el enunciado: **carga, distribución real, casos heterogéneos y comportamiento del sistema**. Cada prueba indica su objetivo, cómo ejecutarla, qué evidencia capturar y una tabla para registrar los resultados.

> **Los resultados deben medirse en el despliegue real** (coordinador más tres workers en computadoras distintas). Las tablas están vacías a propósito: complételas con lo que observen.

## 0. Preparación

1. Levantar el coordinador y los tres workers, cada uno en su PC (ver README).
2. Verificar en el dashboard que los tres workers aparezcan **libres** y con **v3**, y que cada pool tenga al menos un worker.
3. Generar el dataset en la máquina que hará de cliente:
   ```bash
   python scripts/generar_dataset.py
   ```
4. Anotar la composición del dataset (`dataset_prueba/composicion.json`). Después de enviar la carga, la sección **Variedad de archivos recibidos** del dashboard muestra lo mismo en gráficos: tomar una captura como evidencia de la diversidad de tipos, formatos y tamaños.

| Dato | Valor |
|---|---|
| Archivos totales | |
| Casos homogéneos / heterogéneos | |
| Audios / videos / imágenes / no soportados | |
| Livianos / medianos / pesados | |
| Volumen total (MB) | |

5. Anotar el entorno:

| Nodo | Rol | CPU (núcleos) | RAM | SO | Pools | Concurrencia |
|---|---|---|---|---|---|---|
| | Coordinador | | | | — | — |
| | worker-1 | | | | | |
| | worker-2 | | | | | |
| | worker-3 | | | | | |
| Red | | | | | | |

**Evidencia general en todas las pruebas:** capturas del dashboard (colas, workers, trazabilidad) y los archivos CSV/JSON que genera `--metricas`.

---

## P1. Distribución real entre nodos

**Objetivo:** demostrar que las sub-tareas se reparten entre workers en computadoras distintas.

```bash
python cliente/cliente.py carga ./dataset_prueba --concurrentes 4 --metricas pruebas/p1_carga_completa.csv
```

**Evidencia:** captura de la tabla de Workers (columna **Procesadas**, IPs distintas en **Host**), de la línea de tiempo con barras superpuestas y el bloque "Reparto por worker" del resumen.

| Worker (IP) | Sub-tareas | % | Tiempo ocupado (s) | Fallidas |
|---|---|---|---|---|
| | | | | |

---

## P2. Escalabilidad: 1, 2 y 3 workers

**Objetivo:** medir cuánto se acelera el procesamiento al agregar nodos.

Repetir el mismo subconjunto (por ejemplo, 10 casos) con 1, 2 y 3 workers encendidos. Limpiar casos entre corridas si se desea.

```bash
python cliente/cliente.py carga ./dataset_prueba --max-casos 10 --concurrentes 4 --metricas pruebas/p2_1worker.csv
# encender un worker más y repetir con p2_2workers.csv, luego p2_3workers.csv
```

| Workers | Tiempo total (s) | Archivos/min | Speedup global (del resumen) | Speedup vs. 1 worker |
|---|---|---|---|---|
| 1 | | | | 1.00× |
| 2 | | | | |
| 3 | | | | |

**Análisis esperado:** por qué la aceleración no es lineal (subida de archivos, tráfico al coordinador, sub-tareas pesadas que limitan el caso, diferencias de hardware).

---

## P3. Casos heterogéneos y routing por tipo

**Objetivo:** mostrar que un mismo caso genera operaciones distintas que van a pools distintos.

```bash
python cliente/cliente.py enviar ./dataset_prueba/caso_022_heterogeneo
python cliente/cliente.py estado <case-id>
```

**Evidencia:** detalle del caso (columna **Pool**), reporte consolidado (tabla "Por tipo de archivo y operación") y mapa de flujo.

| Archivo (tipo) | Operaciones generadas | Pool | Worker | Resultado |
|---|---|---|---|---|
| | | | | |

---

## P4. Todos genéricos vs. especializados (Unidad 1)

**Objetivo:** respaldar con datos la decisión de usar workers genéricos (ver ARQUITECTURA, sección 4).

- **Config. A, genérico:** los tres workers sin `WORKER_POOLS`.
- **Config. B, especializado:** la PC más potente con `WORKER_POOLS=video WORKER_CONCURRENCY=2`, y las otras dos con `WORKER_POOLS=audio,ligera` (o una de ellas genérica).

Correr la misma carga con cada configuración:

```bash
python cliente/cliente.py carga ./dataset_prueba --max-casos 15 --concurrentes 4 --metricas pruebas/p4_generico.csv
python cliente/cliente.py carga ./dataset_prueba --max-casos 15 --concurrentes 4 --metricas pruebas/p4_especializado.csv
```

| Configuración | Tiempo total (s) | Duración media de caso (s) | Espera máx. en cola ligera | Speedup |
|---|---|---|---|---|
| A, genérico | | | | |
| B, especializado | | | | |

**Análisis:** ¿cuál terminó antes? ¿Hubo nodos ociosos en B mientras otro pool tenía cola? ¿Qué pasa en B si se apaga el nodo de video? Usar estos resultados para justificar la elección de workers genéricos.

---

## P5. Prioridades

**Objetivo:** un caso de prioridad alta adelanta a los que ya estaban en cola.

1. Enviar carga de prioridad baja para que se forme cola:
   ```bash
   python cliente/cliente.py carga ./dataset_prueba --max-casos 8 --concurrentes 4
   ```
2. Mientras hay sub-tareas en espera (tarjetas de colas), enviar un caso urgente:
   ```bash
   python cliente/cliente.py enviar ./dataset_prueba/caso_023_heterogeneo --prioridad 10
   ```

**Evidencia:** en la línea de tiempo (o en los tiempos de inicio del reporte), las sub-tareas del caso urgente empiezan antes que las de casos enviados antes.

| Caso | Prioridad | Enviado (hh:mm:ss) | Primera sub-tarea inicia | Terminó |
|---|---|---|---|---|
| | | | | |

---

## P6. Caída de un worker y redistribución

**Objetivo:** si un nodo se cae a mitad de una sub-tarea, otro la retoma y el caso termina igual.

1. Enviar un caso con videos pesados.
2. Cuando un worker esté procesando un video (detalle del caso con %), cortarlo de golpe: desconectar el cable o Wi-Fi, apagar la PC o ejecutar `docker kill <contenedor>`.
3. Observar que la sub-tarea pasa a otro worker ("redistribuida" en el detalle; en la consola del coordinador aparece `redistribuida: X → Y`) y que a los 15 s el worker caído aparece **desconectado**.

| Sub-tarea | Worker original | % al caer | Worker que la retomó | Tiempo hasta retomarla | Estado final del caso |
|---|---|---|---|---|---|
| | | | | | |

---

## P7. Saturación y reacción a la carga

**Objetivo:** un worker con la CPU al límite deja de tomar trabajo y la carga se redistribuye.

1. En uno de los nodos, generar carga externa de CPU, por ejemplo con `stress --cpu <núcleos>` o abriendo otro proceso pesado.
2. Enviar carga al sistema.
3. Observar que ese worker pasa a **saturado**, que la cola de sus pools muestra un consumidor menos y que el reparto de sub-tareas lo favorece menos. Al quitar la carga externa, vuelve a **libre**.

| Momento | CPU del nodo | Estado | Consumidores en sus colas | Sub-tareas que tomó en el intervalo |
|---|---|---|---|---|
| Antes | | | | |
| Con carga externa | | | | |
| Después | | | | |

---

## P8. Errores, reintentos y barrier/join

**Objetivo:** el caso no se cierra hasta resolver todas sus sub-tareas, y los fallos quedan explicados en el reporte.

- **Formato no soportado / archivo corrupto:** los casos heterogéneos (p. ej. `caso_022`, `caso_025`, `caso_028`) con archivos `*_notas_*.txt/pdf` o `*_corrupto_*.mp4` terminan como **parcialmente completado**, con el motivo en el resumen.
- **Reintentos (error transitorio):** durante una conversión, cortar brevemente la red de un worker (unos segundos). La sub-tarea pasa a **reintentando**, el caso muestra ese estado y luego se recupera.

| Caso | Sub-tareas | Completadas | Fallidas (motivo) | Reintentos | Estado final | ¿Cerró solo al final? |
|---|---|---|---|---|---|---|
| | | | | | | |

---

## P9. Cancelación

**Objetivo:** cancelar un caso en curso detiene el trabajo pendiente.

```bash
python cliente/cliente.py enviar ./dataset_prueba/caso_012_video
python cliente/cliente.py cancelar <case-id>       # mientras procesa
```

| Sub-tareas totales | Completadas antes de cancelar | Canceladas | Tiempo hasta vaciar la cola |
|---|---|---|---|
| | | | |

---

## P10. Generación automática de casos

**Objetivo:** agrupar archivos en casos a partir de metadatos.

```bash
python cliente/cliente.py auto ./dataset_prueba --por evento --concurrentes 3 --metricas pruebas/p10_por_evento.csv
```

**Evidencia:** lista de casos creados (uno por evento) y reporte de uno de ellos con los metadatos de cada archivo.

| Criterio | Casos creados | Archivos | Tiempo total (s) |
|---|---|---|---|
| evento | | | |

---

## Conclusiones

Completar con: qué tan bien escaló el sistema, cuellos de botella observados (red, coordinador, sub-tareas pesadas), qué demostró la comparación genérico vs. especializado, comportamiento ante fallos y posibles mejoras.
