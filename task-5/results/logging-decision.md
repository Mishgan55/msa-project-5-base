# Task 5. Обоснование способа отправки и хранения логов

## Выбранное решение

Приложения пишут structured JSON в stdout/stderr. Fluent Bit собирает контейнерные логи, обогащает их Kubernetes metadata и отправляет в Elasticsearch. Поиск и расследование выполняются в Kibana.

Схема:

```text
Java app / Spring Batch worker
        → stdout JSON
        → container runtime log
        → Fluent Bit DaemonSet/agent
        → Elasticsearch index
        → Kibana / operational dashboards
```

## Почему stdout, а не запись напрямую из приложения

1. **Развязка по доступности.** Elasticsearch может быть временно недоступен; приложение не должно блокировать batch processing из-за logging backend.
2. **Стандарт контейнеров.** Kubernetes и Docker уже собирают stdout/stderr; не нужны sidecar-компоненты в каждом приложении.
3. **Retry и buffering.** Fluent Bit умеет буферизовать и повторно отправлять логи.
4. **Единый формат.** JSON упрощает parsing и поиск по `job_id`, `trace_id`, `error_code`.
5. **Меньше конфигурации в приложении.** Приложению не нужны credentials и адреса Elasticsearch.
6. **Безопасность.** Можно централизованно фильтровать чувствительные поля перед индексацией.

## Почему Fluent Bit

- низкое потребление CPU/RAM;
- поддержка Kubernetes metadata;
- filter/parser/enrichment pipeline;
- в Elasticsearch, Loki, Cloud Logging и другие системы;
- может работать как DaemonSet в Kubernetes или агент на VM.

Если потребуются сложные трансформации, допустимо добавить Logstash между Fluent Bit и Elasticsearch, но это не обязательно на первом этапе.

## Почему Elasticsearch/Kibana

- полнотекстовый поиск и агрегации по structured fields;
- удобный UI для расследования инцидентов;
- mature-экосистема для логов Java-приложений;
- retention/index lifecycle policies;
- хорошо сочетается с observability-стеком Prometheus/Grafana: метрики и alerts в Grafana, детализация по логам в Kibana.

## Индексация и retention

Предлагаемые индексы:

- `tradeware-app-*` — обычные application logs;
- `tradeware-batch-*` — batch worker logs;
- `tradeware-audit-*` — аудит загрузок и статусов заданий.

Рекомендации:

- hot retention для детальных логов — 14–30 дней;
- агрегированные/аудитные события — дольше, в зависимости от compliance;
- включить index lifecycle management и rollover по размеру/возрасту;
- не индексировать stack trace как множество отдельных документов; хранить его одним полем ограниченного размера.

## Контроль качества логов

- единый JSON encoder в Java;
- обязательные поля `timestamp`, `level`, `service`, `version`, `event`, `trace_id`;
- для batch jobs дополнительно `job_id`, `step`, `status`;
- запрет на логирование секретов, токенов, паролей, полного содержимого CSV;
- sampling/level policy для DEBUG в production;
- alert при резком росте ERROR/WARN по workerам.

## Альтернативы

| Вариант | Плюсы | Минусы | Итог |
|---|---|---|---|
| Прямая отправка из приложения в Elasticsearch | Не нужен агент | Блокировки/потеря логов при недоступности ES, credentials в приложении, сложный retry | Не выбран |
| Только файлы логов на VM | Просто и привычно | Нет централизации, сложно искать, файлы теряются при пересоздании контейнеров | Не выбран |
| Grafana Loki | Дёшево, хорошо интегрировано с Grafana | Менее мощный полнотекстовый поиск и аналитика по сравнению с Elasticsearch | Допустимая альтернатива на следующем этапе |
| GCP Cloud Logging | Managed, минимальная эксплуатация | Сильная зависимость от GCP, сложнее переносимая vendor-neutral архитектура | Подходит, если компания окончательно выбирает GCP-native путь |
