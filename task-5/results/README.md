# Task 5 — мониторинг, логирование и оповещение

Состав результата:

- `monitoring-plan.md` — выбранный стек, ключевые метрики, dashboards, alerts, структура логов;
- `logging-decision.md` — обоснование способа отправки логов через stdout JSON → Fluent Bit → Elasticsearch/Kibana;
- `alert-rules.yaml` — пример Prometheus alerting rules;
- `prometheus-scrape-example.yaml` — пример scrape-конфигурации для Kubernetes pods и postgres exporter;
- `c4-observability-architecture.drawio` — редактируемая C4-diagram с мониторингом/логированием/оповещением;
- `c4-observability-architecture.svg` — визуализация диаграммы.
