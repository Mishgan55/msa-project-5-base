# Task 5. Проект мониторинга, оповещения и логирования TradeWare

## Цели

- Обнаруживать деградацию batch processing до массовых обращений пользователей.
- Контролировать SLA: отчёт на 2 000 строк обрабатывается в среднем до 30 секунд.
- Видеть backlog при 100–150 параллельных загрузках.
- Быстро локализовать ошибки файла, строки, шага, workerа и инфраструктуры.
- Сохранять корреляцию между пользовательской загрузкой, batch job, retry и финальным статусом.

## Выбранный стек

### Метрики и alerting

- **Micrometer** в Spring Batch worker и Java-сервисах.
- **Prometheus** для scrape и хранения time series.
- **Alertmanager** для маршрутизации оповещений в email/Slack/incident channel.
- **Grafana** для dashboards и exploration.

Причины: Prometheus/Grafana хорошо подходят для микросервисной архитектуры, имеют развитую интеграцию с Kubernetes и Java/Micrometer, не требуют изменения транспортного протокола приложений.

### Логи

- Приложения пишут structured JSON logs в stdout/stderr.
- **Fluent Bit** собирает логи контейнеров как DaemonSet/agent.
- **Elasticsearch** хранит и индексирует логи.
- **Kibana** используется для поиска, фильтрации и расследования инцидентов.
- Допускается использование Logstash, если потребуется сложная трансформация, но базово Fluent Bit достаточно.

Причины: stdout является стандартом для контейнеров; агентская модель не связывает приложение с доступностью Elasticsearch; буферизация и retry выполняются агентом.

## Ключевые метрики

### 1. SLA и throughput batch processing

| Метрика | Тип | Labels | Зачем нужна |
|---|---|---|---|
| `ingestion_job_duration_seconds` | histogram | `job_type`, `status` | Контроль времени обработки и SLA 30 секунд для типового файла |
| `ingestion_rows_processed_total` | counter | `job_type`, `result` | Throughput и объём успешных/отклонённых строк |
| `ingestion_rows_invalid_total` | counter | `job_type`, `error_code` | Рост ошибок данных и проблемы шаблона/источника |
| `ingestion_files_processed_total` | counter | `status` | Количество успешных/failed/quarantined файлов |
| `ingestion_file_size_bytes` | histogram | `job_type` | Связь размера файла и длительности |
| `ingestion_rows_per_second` | derived/gauge | `job_type` | Фактическая производительность workerов |
| `ingestion_active_workers` | gauge | `worker_pool` | Сколько workers реально обрабатывают задания |

### 2. Очередь и масштабирование

| Метрика | Тип | Labels | Зачем нужна |
|---|---|---|---|
| `ingestion_pending_jobs` | gauge | `job_type` | Backlog заданий |
| `ingestion_oldest_pending_job_age_seconds` | gauge | `job_type` | Насколько долго ждёт самое старое задание |
| `ingestion_running_jobs` | gauge | `job_type` | Параллельная загрузка workerов |
| `ingestion_job_retries_total` | counter | `job_type`, `step` | Частота retry и нестабильность инфраструктуры |
| `ingestion_job_restart_total` | counter | `job_type` | Перезапуски после падений |

### 3. Ошибки и качество данных

| Метрика | Тип | Labels | Зачем нужна |
|---|---|---|---|
| `ingestion_quarantine_files_total` | counter | `reason` | Рост нечитаемых/невалидных файлов |
| `ingestion_invalid_rows_ratio` | derived | `job_type` | Доля брака в данных |
| `ingestion_error_total` | counter | `step`, `error_type` | Локализация шага и типа сбоя |
| `ingestion_validation_errors_total` | counter | `error_code` | Самые частые нарушения формата |

### 4. Ресурсы Java/workerов

| Метрика | Источник | Зачем нужна |
|---|---|---|
| `process_cpu_usage`, `container_cpu_usage_seconds_total` | Micrometer/cAdvisor | CPU saturation |
| `jvm_memory_used_bytes` | Micrometer | Потребление heap/non-heap |
| `jvm_gc_pause_seconds` | Micrometer | GC как причина задержек |
| `jvm_threads_live_threads` | Micrometer | Утечки/насыщение потоков |
| `kube_pod_container_status_restarts_total` | Kubernetes | Нестабильность workerов |

### 5. PostgreSQL и интеграции

| Метрика | Источник | Зачем нужна |
|---|---|---|
| `hikaricp_connections_active` | Micrometer | Насыщение пула соединений |
| `hikaricp_connections_pending` | Micrometer | Ожидание соединений |
| `hikaricp_connections_timeout_total` | Micrometer | Нехватка соединений |
| `pg_database_size_bytes`, replication/latency metrics | postgres exporter/cloud metrics | Состояние БД |
| `gcs_request_errors_total` | custom/client metrics | Ошибки чтения/записи файлов |

## Основные dashboards

### Operations overview

- успешные/failed jobs за последние 1h/24h;
- backlog и возраст самого старого pending job;
- p50/p95/p99 `ingestion_job_duration_seconds`;
- rows/sec и active workers;
- ошибки по шагам;
- состояние CPU/memory/GC/DB pool.

### Data quality dashboard

- invalid rows ratio;
- top error codes;
- quarantine files по складам/источникам;
- повторные загрузки одного файла;
- динамика качества по дням.

### SLA dashboard

- доля файлов до 2 000 строк, обработанных быстрее 30 секунд;
- p95 длительности по размеру файла;
- время от `ACCEPTED` до `SUCCEEDED` включая ожидание в очереди;
- количество параллельных загрузок в пике.

## Alerting rules

Минимальный набор alerts:

1. **BatchJobFailed** — есть failed job за последние 5 минут.
2. **BatchBacklogGrowing** — `ingestion_pending_jobs > 100` в течение 10 минут.
3. **OldestJobWaitingTooLong** — `ingestion_oldest_pending_job_age_seconds > 300` в течение 5 минут.
4. **SmallFileSlaBreached** — p95 длительности файлов до 2 000 строк > 30 секунд в течение 15 минут.
5. **HighInvalidRowsRatio** — доля invalid rows > 5% за 15 минут.
6. **QuarantineGrowing** — quarantine files растут быстрее обычного.
7. **DbPoolSaturation** — active DB connections > 80% или pending > 0 в течение 5 минут.
8. **WorkerRestarting** — рост restarts/OOM kills.
9. **NoSuccessfulIngestion** — нет успешных заданий в течение заданного окна при наличии pending jobs.

Пример правил находится в `alert-rules.yaml`.

## Логируемые события

### Обязательные события

- приём файла монолитом;
- сохранение raw file в GCS;
- создание ingestion job;
- захват job workerом;
- начало/завершение каждого Spring Batch step;
- chunk retry и skip;
- validation error с номером строки и кодом ошибки;
- запись staging и merge результата;
- перевод файла в quarantine;
- финальный статус job;
- отправка уведомления.

### Структура JSON-лога

```json
{
  "timestamp": "2026-01-10T09:00:00.123Z",
  "level": "INFO",
  "service": "inventory-batch-worker",
  "version": "1.0.0",
  "trace_id": "4f6d...",
  "job_id": "ing-20260110-000123",
  "batch_job_execution_id": 8123,
  "step": "validateAndEnrichChunkStep",
  "warehouse_id": 42,
  "file_hash": "sha256:...",
  "rows_in": 2000,
  "rows_valid": 1988,
  "rows_invalid": 12,
  "duration_ms": 18340,
  "event": "job_finished",
  "status": "SUCCEEDED"
}
```

Не логируются персональные данные, секреты, строки подключения и полное содержимое файлов. Для ошибок строки логируются номер строки, код ошибки и ограниченный набор бизнес-полей, необходимый для диагностики.

## Трасcаировка и корреляция

- `trace_id` создаётся при загрузке файла в ERP API и прокидывается в job metadata.
- `job_id` присутствует во всех логах workerа.
- Метрики дополняются labels `job_type`, `status`, `step`, но не содержат high-cardinality `job_id` или `file_hash`.
- В Kibana можно перейти от alert/incident к логам конкретного `job_id`.
