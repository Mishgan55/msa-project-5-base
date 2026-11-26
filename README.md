# MSA Project 5 — пакетная обработка, Kubernetes Jobs и наблюдаемость

Репозиторий содержит решения пяти заданий проектной работы.

## Структура

| Директория | Содержание |
|---|---|
| `task-1/results` | Выбор и локальный POC Apache Airflow для пакетной обработки маркетинговых данных |
| `task-2/results` | Дизайн модуля ежедневной генерации B2B-прайс-листов, сравнительная таблица и C4 To Be |
| `task-3/results` | Docker-образ PostgreSQL exporter и Kubernetes CronJob для ежедневной выгрузки shipments |
| `task-4/results` | ADR по внедрению Spring Batch в TradeWare и C4-архитектура решения |
| `task-5/results` | Проект мониторинга, логирования и alerting для TradeWare |

В каждой директории `results/README.md` есть состав артефактов и инструкции по использованию.

## Быстрые проверки

### Task 1: Airflow POC

```bash
cd task-1/results/airflow
cp .env.example .env
docker compose up -d
docker compose exec airflow-scheduler airflow dags trigger customer_delivery_report_batch
```

Airflow UI — http://localhost:8080, Mailpit UI — http://localhost:8025.

### Task 3: Kubernetes CronJob в Minikube

```bash
cd task-3/results
./run-minikube-demo.sh
```

Скрипт поднимает Minikube, собирает локальный Docker-образ, разворачивает одноразовый PostgreSQL с синтетическими данными, применяет CronJob и запускает ручной Job для проверки.
