# Plataforma Distribuida de Procesamiento Multimedia por Casos
### IC-6600 · Principios de Sistemas Operativos — TEC Campus San Carlos · II Semestre 2026

## Descripción

La plataforma recibe **casos**: conjuntos de archivos de video, audio e imágenes, por ejemplo el material de un evento. Cada caso se divide en sub-tareas (conversión de un video, extracción de un audio, búsqueda de los metadatos de una canción…) que se distribuyen entre **workers ubicados en computadoras distintas**, los cuales las procesan de forma concurrente. Cuando todas las sub-tareas de un caso finalizan, el sistema genera un **reporte consolidado**. Un **dashboard** web muestra en tiempo real qué computadora procesó cada tarea, el uso de CPU de cada una y el avance de cada caso.

| Documento | Contenido |
|---|---|
| [docs/ARQUITECTURA.md](docs/ARQUITECTURA.md) | Arquitectura del sistema y decisiones de diseño |
| [docs/MANUAL_USUARIO.md](docs/MANUAL_USUARIO.md) | Uso del dashboard y del cliente |
| [docs/Informe_de_pruebas.docx](docs/Informe_de_pruebas.docx) | Resultados de las pruebas en el despliegue real |

## Componentes

```
          Navegador o cliente.py  →  envían casos
                       │
                       ▼
┌──────────── Computadora del coordinador ─────────────┐
│  Coordinador (distribución y consolidación)          │
│  PostgreSQL (estado)   RabbitMQ (colas de tareas)    │
│  uploads/ (originales)   results/ (resultados)       │
└──────────────────────────────────────────────────────┘
        │                │                │                │
        ▼                ▼                ▼                ▼
   worker-1 (PC 1)  worker-2 (PC 2)  worker-3 (PC 3)  worker-4 (PC 4)
   misma PC del     red del TEC      red del TEC      otra red, por
   coordinador                                        VPN Tailscale
```

| Directorio / archivo | Descripción |
|---|---|
| `coordinador/app.py` | Coordinador: recepción de casos, distribución, consolidación de resultados, dashboard y reportes |
| `worker/worker.py` | Worker que se ejecuta en cada computadora (incluye `Dockerfile`) |
| `cliente/cliente.py` | Cliente de terminal para enviar casos y medir tiempos |
| `scripts/descargar_dataset.py` | Genera el dataset de prueba con archivos reales descargados de internet |
| `scripts/generar_dataset.py` | Genera un dataset de prueba sin conexión (archivos sintéticos) |
| `scripts/prueba_prioridad.py` | Prueba de prioridades entre casos |
| `docker-compose.yml` | Inicia RabbitMQ y PostgreSQL |
| `init.sql` | Crea las tablas de la base de datos |

---

## 1. Inicio del coordinador

En la computadora que funciona como coordinador (requiere Docker y Python 3.11 o superior):

```bash
cd Proyecto
docker compose up -d                   # inicia RabbitMQ y PostgreSQL
python3 -m venv .venv                  # solo la primera vez
source .venv/bin/activate
pip install -r coordinador/requirements.txt -r worker/requirements.txt   # solo la primera vez
python coordinador/app.py
```

- **Dashboard:** `http://localhost:8000`
- **Panel de RabbitMQ:** `http://localhost:15672` (usuario `admin`, contraseña `admin123`)
- **IP que utilizan los workers:** se obtiene con `hostname -I` (Linux) o `ipconfig` (Windows).

---

## 2. Inicio de un worker en cada computadora

Cada computadora que aporta un worker necesita el directorio `worker/`. El despliegue utilizado consta de cuatro: la computadora del coordinador (worker-1), dos en la red del TEC (worker-2 y worker-3) y una conectada desde otra red mediante Tailscale (worker-4). Cada worker debe tener un **identificador único**.

**Con Docker** (se ejecuta en primer plano; se detiene con Ctrl+C):

```bash
cd worker
docker build -t worker .               # la primera vez y cada vez que cambie worker.py
docker run --rm --name worker-2 -e HOST_NAME=$(hostname) -e COORDINATOR_IP=192.168.1.100 -e WORKER_ID=worker-2 worker
```

- `192.168.1.100` se reemplaza por la IP del coordinador y `worker-2` por el identificador del worker.
- `HOST_NAME=$(hostname)` permite que el dashboard muestre el nombre real del equipo.
- **En la computadora del coordinador** se debe agregar `--network host` y usar `COORDINATOR_IP=localhost`.
- Si Linux muestra `permission denied ... docker.sock`, se debe anteponer `sudo` a `docker`.

**Sin Docker** (requiere FFmpeg instalado):

```bash
pip install -r worker/requirements.txt
cd worker
COORDINATOR_IP=192.168.1.100 WORKER_ID=worker-2 python worker.py
```

**Opciones del worker** (se definen con `-e NOMBRE=valor`; los valores por defecto son suficientes):

| Opción | Valor por defecto | Descripción |
|---|---|---|
| `WORKER_POOLS` | `video,audio,ligera` | Tipos de tarea que atiende (los tres: worker genérico) |
| `WORKER_CONCURRENCY` | `1` | Cantidad de tareas simultáneas |
| `CPU_HIGH` / `CPU_LOW` | `80` / `60` | Porcentaje de CPU a partir del cual deja de solicitar trabajo y por debajo del cual lo retoma |

Si la computadora no está en la misma red que el coordinador, la conexión se puede establecer mediante **Tailscale**, una VPN gratuita.

---

## 3. Prueba del sistema

```bash
# Generar el dataset (~480 archivos reales, ~9 GB, con 10 archivos de 400–600 MB)
python scripts/descargar_dataset.py

# Enviar un caso
python cliente/cliente.py enviar ./dataset_real/caso_022_heterogeneo

# Enviar todos los casos, 4 a la vez, guardando los tiempos para el informe
python cliente/cliente.py carga ./dataset_real --concurrentes 4 --metricas pruebas/carga.csv

# Consultar el estado general
python cliente/cliente.py resumen
```

Si el cliente se ejecuta en otra computadora, primero se define la dirección del coordinador: `export COORDINATOR_URL=http://<IP del coordinador>:8000`.

---

## Funcionalidades

- **Casos heterogéneos:** a cada archivo se le aplican las operaciones que corresponden a su tipo.
  - **Video:** conversión de formato, extracción del audio y selección de una portada.
  - **Canción:** búsqueda de álbum, fecha, género y letra en servicios en línea.
  - **Audio:** conversión a mp3.
  - **Imagen:** generación de una miniatura.
- **Distribución automática:** las sub-tareas esperan en colas y las procesa el primer worker disponible. Los casos de prioridad alta se procesan antes.
- **Sincronización (barrier/join):** un caso se cierra únicamente cuando finalizan todas sus sub-tareas.
- **Tolerancia a fallos:** si un worker falla, otro retoma su tarea. Los errores de red se reintentan.
- **Control de casos:** los casos se pueden **pausar, reanudar y cancelar** desde el dashboard o el cliente.
- **Monitoreo:** CPU, RAM y estado de cada worker, equipo de origen y pausa automática ante saturación de CPU.
- **Resultados y reportes:** todos los resultados se descargan desde el dashboard, y cada caso tiene un reporte consolidado.

## Tecnologías

| Función | Tecnología |
|---|---|
| Lenguaje | Python 3.11+ |
| Colas de tareas | RabbitMQ |
| Base de datos | PostgreSQL |
| Coordinador y dashboard | FastAPI + HTML/JavaScript |
| Procesamiento de audio y video | FFmpeg |
| Medición de CPU y RAM | psutil |
| Contenedores | Docker |
