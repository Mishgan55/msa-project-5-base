# Task 3 — Distributed Scheduling с Kubernetes CronJob

Состав результата:

- `app/export_shipments.py` — exporter одной таблицы PostgreSQL в CSV через streaming `COPY ... TO STDOUT`;
- `app/Dockerfile` — минимальный non-root Docker-образ;
- `app/requirements.txt` — зависимости;
- `k8s/` — namespace, демонстрационный PostgreSQL, ConfigMap, PVC и CronJob; локальные demo Secrets создаются скриптом во время запуска и не публикуются в репозитории;
- `run-minikube-demo.sh` — автоматизированный локальный сценарий Minikube;
- `evidence/minikube-cronjob-evidence.png` — сводный скриншот реального запуска в Minikube;
- `evidence/*.txt` — выводы kubectl, логи Job и содержимое экспортированного CSV;
- `evidence/minikube-cronjob-evidence.html` — интерактивная версия evidence-отчёта.

## Что делает решение

CronJob `shipment-exporter` ежедневно в 20:00 по Europe/Moscow запускает контейнер exporter. Контейнер читает таблицу `shipments` из PostgreSQL и сохраняет CSV на PersistentVolumeClaim `analytics-exports`. Логи в формате JSON попадают в stdout и могут собираться централизованно.

Настройки запуска:

- `concurrencyPolicy: Forbid` — не допускает параллельных дублей;
- `backoffLimit: 2` — retry упавшего pod;
- `activeDeadlineSeconds: 1800` — защита от зависшего запуска;
- `ttlSecondsAfterFinished: 86400` — автоматическая очистка завершённых job;
- `requests/limits` — контроль потребления ресурсов;
- non-root security context и `readOnlyRootFilesystem`.

## Локальный запуск в Minikube

```bash
cd task-3/results
./run-minikube-demo.sh
```

Если в системе установлен Minikube не той архитектуры или бинарник находится не в `PATH`, путь можно передать явно:

```bash
MINIKUBE_BIN=/path/to/minikube ./run-minikube-demo.sh
```

Скрипт:

1. запускает Minikube с docker driver;
2. переключает Docker CLI на Docker daemon внутри Minikube;
3. собирает образ `shipment-exporter:local`;
4. генерирует одноразовый локальный пароль и создаёт Kubernetes Secrets только внутри Minikube;
5. применяет Kubernetes manifests;
6. ждёт готовности PostgreSQL;
7. создаёт ручной Job из CronJob;
8. ждёт завершения и показывает логи/объекты.

Ручной запуск без ожидания cron-расписания:

```bash
kubectl -n logistics-export create job shipment-exporter-manual --from=cronjob/shipment-exporter
kubectl -n logistics-export wait --for=condition=complete job/shipment-exporter-manual --timeout=180s
kubectl -n logistics-export logs job/shipment-exporter-manual
```

Проверка CronJob:

```bash
kubectl -n logistics-export get cronjob shipment-exporter
kubectl -n logistics-export describe cronjob shipment-exporter
```
