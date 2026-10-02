-- ============================================================
-- Se ejecuta automáticamente cuando PostgreSQL arranca
-- por primera vez (docker-compose up)
-- ============================================================

CREATE TABLE IF NOT EXISTS workers (
    worker_id       VARCHAR(50)     PRIMARY KEY,
    worker_type     VARCHAR(30)     DEFAULT 'generic',
    host_address    VARCHAR(255)    NOT NULL,
    status          VARCHAR(20)     DEFAULT 'idle',
    cpu_usage       DECIMAL(5,2)    DEFAULT 0,
    memory_usage    DECIMAL(5,2)    DEFAULT 0,
    active_tasks    INT             DEFAULT 0,
    last_heartbeat  TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    pools           VARCHAR(100),                -- pools que atiende: video,audio,ligera
    concurrency     INT             DEFAULT 1    -- sub-tareas en paralelo
);

CREATE TABLE IF NOT EXISTS cases (
    case_id         VARCHAR(50)     PRIMARY KEY,
    case_name       VARCHAR(200),
    priority        INT             DEFAULT 5,
    status          VARCHAR(30)     DEFAULT 'queued',
    total_subtasks  INT             DEFAULT 0,
    completed_count INT             DEFAULT 0,
    failed_count    INT             DEFAULT 0,
    created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP,
    metadata        JSONB                        -- metadatos del caso y de cada archivo
);

CREATE TABLE IF NOT EXISTS subtasks (
    subtask_id      VARCHAR(50)     PRIMARY KEY,
    case_id         VARCHAR(50)     REFERENCES cases(case_id),
    file_name       VARCHAR(300)    NOT NULL,
    file_path       VARCHAR(500)    NOT NULL,
    file_type       VARCHAR(20)     NOT NULL,
    operation       VARCHAR(50)     NOT NULL,
    target_format   VARCHAR(10),
    status          VARCHAR(20)     DEFAULT 'pending',
    progress        DECIMAL(5,2)    DEFAULT 0,
    assigned_worker VARCHAR(50),
    error_message   TEXT,
    result_path     VARCHAR(500),
    pool            VARCHAR(20),                 -- cola a la que se envió
    retries         INT             DEFAULT 0,   -- reintentos por errores transitorios
    reassignments   INT             DEFAULT 0,   -- veces que pasó a otro worker
    created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    assigned_at     TIMESTAMP,
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP
);

CREATE TABLE IF NOT EXISTS resource_logs (
    id              SERIAL          PRIMARY KEY,
    worker_id       VARCHAR(50)     REFERENCES workers(worker_id),
    cpu_usage       DECIMAL(5,2),
    memory_usage    DECIMAL(5,2),
    active_tasks    INT,
    recorded_at     TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_subtasks_case ON subtasks(case_id);
CREATE INDEX IF NOT EXISTS idx_subtasks_status ON subtasks(status);
CREATE INDEX IF NOT EXISTS idx_cases_status ON cases(status);
