# Task 1. Выбор решения для пакетной обработки данных

## Выбранное решение

Для пакетной обработки данных маркетингового контура выбран **Apache Airflow 2.x**.

Airflow подходит как оркестратор batch-пайплайнов, потому что объединяет декларативное описание DAG в Python, планировщик, исполнителей задач, веб-интерфейс, retry-механизмы, зависимости между шагами, ветвления, XCom для передачи небольших результатов между задачами и большой каталог готовых интеграций.

Ожидаемый объём — около 1 млн строк за запуск. Airflow сам не обязан обрабатывать весь массив данных в памяти: он должен оркестрировать внешние вычисления в Spark, BigQuery, Redshift, PostgreSQL или отдельных worker-процессах. Такой подход не перегружает scheduler/webserver и позволяет масштабировать обработку независимо от оркестрации.

## Сравнение с альтернативами

| Критерий | Apache Airflow | cron + скрипты | Spring Batch | Apache NiFi |
|---|---|---|---|---|
| Сложные зависимости и ветвления | Да, DAG, BranchPythonOperator, условные зависимости | Нужно программировать вручную | Есть flow/step transitions, но оркестрация многих job слабее | Да, визуальные flow |
| Готовые интеграции | Очень много provider-пакетов | Нет | В основном Java/JDBC/файлы | Много процессоров |
| Retry, SLA, alerting | Из коробки | Нужно делать самостоятельно | Retry есть, alerting внешний | Есть, но модель отличается |
| Подходит для 1 млн строк | Да, если тяжёлая обработка выносится в Spark/БД | Зависит от реализации | Да для JVM batch | Да, но больше ориентирован на dataflow |
| Эксплуатация в облаке | Managed Airflow, MWAA, Cloud Composer, self-managed в Kubernetes | Простая, но плохо управляемая | Требуется приложение и job launcher | Требует отдельного кластера |
| Навыки команды и сопровождение | Python, декларативные DAG | Python/Shell | Java | Часто визуальная разработка |

Вывод: cron и Spring Batch могут решать отдельные частные задачи, но маркетинговому контуру требуется именно **оркестрация разнородных источников и внешних систем**. Apache NiFi силён в потоковой маршрутизации данных, однако для регулярных batch-пайплайнов с явными зависимостями, расписаниями, backfill и интеграциями Airflow обычно проще и предсказуемее.

## Интеграции с BigQuery, Redshift, Kafka и Spark

Для Airflow существуют официальные и поддерживаемые community provider-пакеты:

| Система | Интеграция | Что ускоряет разработку |
|---|---|---|
| BigQuery | `apache-airflow-providers-google` | `BigQueryExecuteQueryOperator`, `BigQueryInsertJobOperator`, перенос из GCS/в GCS, сенсоры, проверки данных |
| Redshift | `apache-airflow-providers-amazon` | `RedshiftSQLOperator`, `RedshiftDataOperator`, загрузка через `COPY` из S3, выгрузка в S3 |
| Kafka | `apache-airflow-providers-apache-kafka` | producers/consumers, trigger/sensor-механизмы; для массовой обработки Kafka обычно подключается через Spark/Flink или consumer-service |
| Spark | `apache-airflow-providers-apache-spark` | `SparkSubmitOperator`, `SparkSqlOperator`, запуск spark-submit на YARN/Kubernetes/Standalone |
| PostgreSQL/S3/HTTP | PostgreSQL/Amazon/HTTP providers | Готовые hook/operator/sensor-компоненты для JDBC, S3 и внешних API |

Наличие providers существенно ускоряет разработку: подключение, авторизация, ожидание завершения внешней операции и повторное использование соединений уже реализованы.

## Ветвления, условные операторы и event-triggers

Airflow поддерживает:

- `BranchPythonOperator` — выбор следующей ветки по результатам Python-функции;
- `ShortCircuitOperator` — остановку downstream-цепочки по условию;
- `trigger_rule` — запуск задачи при success/failure/skipped/one_failed и других состояниях upstream;
- `Sensor` и `Trigger` — ожидание файла, строки в БД, сообщения, статуса внешнего job;
- event-driven запуск через внешний вызов REST API / CLI, а в Airflow 2.x также через deferrable operators и event-driven компоненты интеграций;
- динамическое создание задач через Task Mapping для обработки списка файлов или партиций.

## Fallback logic, retry и email-уведомления

Из коробки доступны:

- `retries`, `retry_delay`, `retry_exponential_backoff`, `max_retry_delay`;
- `depends_on_past`, `wait_for_downstream`, ограничения parallelism/concurrency;
- fallback-сценарии через ветвления, `trigger_rule`, отдельные cleanup/quarantine/compensation tasks;
- email при failure/retry через `email_on_failure`, `email_on_retry`, `email`;
- явные email-задачи через `EmailOperator`;
- SLA и callback-функции для интеграции с внешним alerting.

В POC настроены retry для шагов, ветвление по качеству и объёму данных, fallback-ветка `quarantine_source` и уведомления об успешном/проблемном завершении через SMTP-совместимый Mailpit.

## Развёртывание в облачной среде

Рекомендуемая целевая схема:

1. Airflow разворачивается в Kubernetes или используется managed-сервис:
   - GCP — Cloud Composer;
   - AWS — MWAA;
   - vendor-neutral — self-managed Airflow в Kubernetes с `KubernetesExecutor` или `CeleryKubernetesExecutor`.
2. Метаданные Airflow хранятся в управляемой PostgreSQL.
3. DAG и зависимости поставляются через CI/CD: контейнерные образы workerов, версия DAG в Git, автоматические проверки синтаксиса и unit-тесты.
4. Тяжёлая обработка 1 млн строк выносится:
   - в Spark on Kubernetes/Dataproc/EMR;
   - либо в BigQuery/Redshift SQL jobs.
5. Секреты хранятся в Secret Manager/Vault/Kubernetes Secrets, а не в DAG.
6. Логи отправляются в централизованное хранилище, метрики — в Prometheus/managed monitoring, alerting — в email/Slack/incident-management.

Для локального POC используется Docker Compose: SQLite-файл для метаданных Airflow и Mailpit для перехвата email. Такая схема выбрана только для простоты локальной демонстрации; в облачной эксплуатации метаданные должны быть вынесены в управляемую PostgreSQL, а worker-процессы должны масштабироваться независимо.

## POC

Реализация находится в каталоге `airflow/`:

- `docker-compose.yml` — локальное развёртывание Airflow и Mailpit;
- `dags/customer_report_batch.py` — DAG с чтением CSV, анализом, ветвлением, retry и уведомлениями;
- `data/delivery_statuses.csv` — валидный входной файл;
- `data/delivery_statuses_invalid.csv` — файл для проверки fallback-ветки;
- `output/` — результат выполнения.

### Пайплайн

1. `read_source` читает CSV и формирует профиль данных.
2. `branch_on_data_quality` проверяет обязательные колонки и пустые значения:
   - при ошибках запускается fallback `quarantine_source` и failure-уведомление;
   - при успешной валидации запускается `analyze_data`.
3. `analyze_data` агрегирует статусы доставки.
4. `branch_on_volume` выбирает ветку `process_large_batch` или `process_small_batch` по порогу `POC_ROW_THRESHOLD`.
5. Выбранная обработка сохраняет JSON-отчёт.
6. `notify_success` отправляет email об успехе.

### Запуск

```bash
cd task-1/results/airflow
docker compose up -d
```

Интерфейсы:

- Airflow UI: http://localhost:8080
- Mailpit UI: http://localhost:8025

После появления DAG `customer_delivery_report_batch` его нужно запустить вручную через UI или командой:

```bash
docker compose exec airflow-scheduler airflow dags trigger customer_delivery_report_batch
```

Для проверки fallback-ветки нужно заменить `data/delivery_statuses.csv` содержимым `data/delivery_statuses_invalid.csv` и повторно запустить DAG.
