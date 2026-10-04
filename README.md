# Plataforma Distribuida de Procesamiento Multimedia por Casos
### IC-6600 · Principios de Sistemas Operativos — TEC Campus San Carlos · II Semestre 2026

## ¿Qué hace?

Recibe **casos**: grupos de archivos de video, audio e imágenes, por ejemplo todo el material de un evento. Divide cada caso en tareas chicas (convertir un video, extraer un audio, buscar los datos de una canción…) y las reparte entre **workers que corren en computadoras distintas**, que las procesan al mismo tiempo. Cuando todas las tareas de un caso terminan, genera un **reporte**. Un **dashboard** web muestra todo en vivo: qué computadora hizo qué, cuánta CPU usa cada una y cómo avanza cada caso.

| Documento | Para qué sirve |
|---|---|
| [docs/ARQUITECTURA.md](docs/ARQUITECTURA.md) | Cómo está armado el sistema y por qué |
| [docs/MANUAL_USUARIO.md](docs/MANUAL_USUARIO.md) | Cómo usar el dashboard y el cliente |
| [docs/INFORME_PRUEBAS.md](docs/INFORME_PRUEBAS.md) | Qué pruebas hacer y dónde anotar los resultados |

## Las piezas

```
         Navegador o cliente.py  →  suben casos
                      │
                      ▼
┌──────────── Computadora del coordinador ────────────┐
│  Coordinador (reparte y junta)    PostgreSQL (estado) │
│  RabbitMQ (filas de espera de tareas)                │
│  uploads/ (originales)   results/ (resultados)       │
└──────────────────────────────────────────────────────┘
        │                     │                    │
        ▼                     ▼                    ▼
   worker-1 (PC 1)       worker-2 (PC 2)      worker-3 (PC 3)
```

| Carpeta / archivo | Qué es |
|---|---|
| `coordinador/app.py` | El coordinador: recibe casos, reparte, junta resultados, dashboard y reportes |
| `worker/worker.py` | El worker que corre en cada computadora (incluye `Dockerfile`) |
| `cliente/cliente.py` | Programa de terminal para mandar casos y medir tiempos |
| `scripts/descargar_dataset.py` | Arma el dataset de prueba con archivos reales de internet |
| `scripts/generar_dataset.py` | Arma un dataset de prueba sin internet (archivos sintéticos) |
| `docker-compose.yml` | Enciende RabbitMQ y PostgreSQL |
| `init.sql` | Crea las tablas de la base de datos |

---

## 1. Encender el coordinador

En la computadora que hace de coordinador (necesita Docker y Python 3.11 o más nuevo):

```bash
cd Proyecto
docker compose up -d                   # enciende RabbitMQ y PostgreSQL
python3 -m venv .venv                  # solo la primera vez
source .venv/bin/activate
pip install -r coordinador/requirements.txt -r worker/requirements.txt   # solo la primera vez
python coordinador/app.py
```

- **Dashboard:** `http://localhost:8000`
- **Panel de RabbitMQ:** `http://localhost:15672` (usuario `admin`, contraseña `admin123`)
- **La IP que van a usar los workers:** `hostname -I` (Linux) o `ipconfig` (Windows).

---

## 2. Encender un worker en cada computadora

Cada integrante necesita la carpeta `worker/` en su computadora. Cada worker tiene que tener un **nombre distinto**.

**Con Docker** (queda en la terminal; se apaga con Ctrl+C):

```bash
cd worker
docker build -t worker .               # la primera vez, y cada vez que cambie worker.py
docker run --rm --name worker-2 -e HOST_NAME=$(hostname) -e COORDINATOR_IP=192.168.1.100 -e WORKER_ID=worker-2 worker
```

- Cambiá `192.168.1.100` por la IP del coordinador y `worker-2` por el nombre de tu worker.
- `HOST_NAME=$(hostname)` hace que el dashboard muestre el nombre real de tu computadora.
- **En la misma computadora del coordinador**, usá `--network host` y `COORDINATOR_IP=localhost`.
- Si Linux dice `permission denied ... docker.sock`, poné `sudo` delante de `docker`.

**Sin Docker** (necesita FFmpeg instalado):

```bash
pip install -r worker/requirements.txt
cd worker
COORDINATOR_IP=192.168.1.100 WORKER_ID=worker-2 python worker.py
```

**Opciones del worker** (se pasan con `-e NOMBRE=valor`; no hace falta tocarlas):

| Opción | Por defecto | Para qué |
|---|---|---|
| `WORKER_POOLS` | `video,audio,ligera` | Qué tipos de tarea atiende (los tres = hace de todo) |
| `WORKER_CONCURRENCY` | `1` | Cuántas tareas hace a la vez |
| `CPU_HIGH` / `CPU_LOW` | `80` / `60` | Con qué % de CPU deja de pedir trabajo y con cuánto vuelve |

Si la computadora no está en la misma red que el coordinador, se pueden conectar con **Tailscale**, una red privada gratuita.

---

## 3. Probar el sistema

```bash
# Armar el dataset (~480 archivos reales, ~9 GB, con 10 archivos de 400–600 MB)
python scripts/descargar_dataset.py

# Mandar un caso
python cliente/cliente.py enviar ./dataset_real/caso_022_heterogeneo

# Mandar todos, 4 a la vez, guardando tiempos para el informe
python cliente/cliente.py carga ./dataset_real --concurrentes 4 --metricas pruebas/carga.csv

# Ver el estado general
python cliente/cliente.py resumen
```

Si el cliente corre en otra computadora: `export COORDINATOR_URL=http://<IP del coordinador>:8000`.

---

## Qué incluye

- **Casos mezclados:** a cada archivo se le hace lo que corresponde a su tipo.
  - **Video:** se convierte de formato, se le extrae el audio y se elige una portada.
  - **Canción:** se buscan su álbum, fecha, género y letra en internet.
  - **Audio:** se convierte a mp3.
  - **Imagen:** se hace una miniatura.
- **Reparto automático:** las tareas esperan en colas y las toma el worker que se libera primero. Los casos urgentes (prioridad alta) pasan adelante.
- **Cierre correcto (barrier/join):** un caso se da por terminado recién cuando terminan todas sus tareas.
- **Tolerancia a fallos:** si un worker se cae, otro retoma su tarea. Los errores de red se reintentan.
- **Control de casos:** se pueden **pausar, reanudar y cancelar** desde el dashboard o el cliente.
- **Monitoreo:** CPU, RAM y estado de cada worker, de qué computadora viene cada uno y pausa automática si una computadora se satura.
- **Resultados y reportes:** todo se descarga desde el dashboard, y cada caso tiene un reporte con su resumen.

## Tecnologías

| Para | Se usa |
|---|---|
| Programar | Python 3.11+ |
| Colas de tareas | RabbitMQ |
| Base de datos | PostgreSQL |
| Coordinador y dashboard | FastAPI + HTML/JavaScript |
| Procesar audio y video | FFmpeg |
| Medir CPU y RAM | psutil |
| Contenedores | Docker |
