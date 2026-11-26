# Task 1 — пакетная обработка данных

Состав результата:

- `rationale.md` — обоснование выбора Apache Airflow, интеграции, retry/fallback/email, облачное развёртывание;
- `airflow/docker-compose.yml` — локальная инсталляция Airflow + Mailpit;
- `airflow/dags/customer_report_batch.py` — POC-пайплайн;
- `airflow/data/` — входные данные для успешного и fallback-сценария;
- `evidence/airflow-poc-evidence.png` — сводный скриншот реального локального запуска;
- `evidence/dag-structure.svg` — схема ветвлений DAG;
- `evidence/mailpit-ui.png` — скриншот Mailpit с письмами;
- `evidence/airflow-ui-login.png` — скриншот локально поднятого Airflow UI;
- `evidence/*.txt`, `evidence/*.json` — выводы команд, состояния задач и ответы API.

Быстрый запуск:

```bash
cd airflow
cp .env.example .env
# при желании замените пароль в .env на собственный локальный
docker compose up -d
```

После старта:

- Airflow UI: http://localhost:8080 — пользователь `admin`, пароль из локального `.env`;
- Mailpit UI: http://localhost:8025

Запуск DAG из терминала:

```bash
docker compose exec airflow-scheduler airflow dags trigger customer_delivery_report_batch
```

В POC были проверены обе ветки:

1. валидный файл — выбрана ветка `process_large_batch`, создан `output/customer_delivery_report_large.json`, отправлено success email;
2. невалидный файл — выбрана fallback-ветка `quarantine_source`, файл скопирован в `output/quarantine/`, отправлено failure email.

Для повторной проверки fallback-ветки замените `data/delivery_statuses.csv` содержимым `data/delivery_statuses_invalid.csv` и запустите DAG ещё раз.
