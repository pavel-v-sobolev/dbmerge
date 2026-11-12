# Tracking и витрины (marts)

DBMerge синхронизирует данные в таблицу: вставляет недостающие, обновляет изменившиеся, помечает или удаляет лишние. Этот документ описывает две **необязательные** надстройки поверх merge:

- **Tracking** — журнал merge в базе, по которому можно узнать, когда таблица менялась и не идёт ли merge прямо сейчас.
- **Mart (витрина)** — цикл, который пересчитывает производную таблицу, когда изменились исходные, и ничего не делает, когда изменений нет.

Если надстройки не нужны, dbmerge работает как раньше — они включаются явно.

## Общая картина

```
merge источника ──► строка в tracking-таблице ──► MartLoop видит изменение ──► пересчитывает витрину
                     (started/finished/changed/boundary)
```

- Каждый merge, обёрнутый в `tracker.track(...)`, оставляет в tracking-таблице строку.
- Пока merge идёт, строка «живая» (heartbeat обновляется) и держит границу безопасного чтения.
- По завершении строка получает `finished_at` и флаг `changed` («изменились ли строки»).
- `MartLoop` опрашивает эту таблицу и запускает пересчёт витрины, только если источники реально менялись.

## Tracking

### Зачем

Две программы работают независимо: одна пишет в таблицы (репликатор, загрузчик), другая считает по ним отчёты (витрина). Второй нужно знать:

1. «Изменилась ли таблица X после момента Y?»
2. «Не пишет ли сейчас кто-то в X прямо в эту секунду?» — чтобы не прочитать строку наполовину.

Ответ на оба вопроса даёт одна tracking-таблица.

### Где ведётся журнал

Журнал живёт в **транзакционной** базе. Для ClickHouse или DuckDB трекер не создаётся — там нет настоящих транзакций или общей для процессов БД; витрину в ClickHouse трекают через PostgreSQL:

```python
tracker = MergeTracker(postgres_engine)              # журнал в PostgreSQL
with dbmerge(clickhouse_engine, table_name="mart",   # сам merge — в ClickHouse
             data=rows, key=["id"]) as merge:
    tracker.track(merge)
```

Чтобы одинаковые имена таблиц из разных баз не смешивались, каждая строка журнала хранит, где живёт слитая таблица: `database_type`, `server`, `database_name`. Их берёт `database_of(engine)`; методы чтения принимают фильтр `database=...`.

### Минимальный пример

```python
from dbmerge import dbmerge
from dbmerge.tracking import MergeTracker

tracker = MergeTracker(engine)          # таблица dbmerge_tracking создаётся сама; трекер живёт долго
with dbmerge(engine, table_name="orders", data=orders_rows,
             key=["id"], merged_on_field="merged_on") as merge:
    track_record, merge_result = tracker.track(merge)
```

`track(merge)` сам вызывает `merge.exec()` и возвращает `(track_record, merge_result)`:

- `track_record` — запись tracking-таблицы об этом merge (`MergeRecord`);
- `merge_result` — обычный результат `exec()` (`mergeResult`).

Параметры выполнения (`commit_all_steps`, `chunk_size`, `bulk_load`) задаются при создании merge — конструктор `dbmerge` их теперь принимает; в `exec()` они тоже остались, для обратной совместимости. Условия, которые строятся на таблицах самого merge (`source_condition`, `delete_condition`, `update_condition`, `insert_condition`), передаются через `track`:

```python
with dbmerge(engine, table_name="orders",
             source_table_name="orders_source", source_schema="source",
             key=["id"], merged_on_field="merged_on") as merge:
    track_record, merge_result = tracker.track(
        merge,
        source_condition=merge.source_table.c["Period"] == period,
    )
```

Имя таблицы берётся из самого `merge`, флаг «изменились ли строки» — из его результата.

### Что можно спросить

```python
# До какого момента данные точно «устоялись» (незавершённые merge не пропускает)?
boundary = tracker.boundary(["orders", "order_items"])   # datetime

# Когда таблицу меняли в последний раз и менялись ли строки?
last = tracker.last_merge("orders")                       # MergeRecord | None
last.started_at, last.finished_at, last.changed

# Граница последнего успешного обновления витрины (её «водяной знак»)?
tracker.watermark("orders_by_customer")                   # datetime | None

# Были ли реальные изменения после момента since?
tracker.changed_since(["orders", "order_items"], since=last_run_at)  # bool
```

- `boundary(...)` — безопасная граница: до этого момента всё закоммичено и согласовано. Это предел для **водяного знака**, а не для чтения (читать можно и дальше).
- `changed_since(...)` — было ли реальное изменение после `since`. Merge, который ничего не изменил (no-op), изменением не считается.

### Как это хранится

Одна таблица `dbmerge_tracking` (имя настраивается):

| колонка | смысл |
|---|---|
| `table_name` | какая таблица мержилась |
| `started_at` / `finished_at` | начало / конец merge |
| `heartbeat_at` | «я ещё жив» — обновляется, пока merge идёт |
| `changed` | изменились ли строки (ставится при завершении) |
| `boundary` | для витрин: окно, обработанное этим merge |

Правило хранения: **для каждой таблицы — не больше одной завершённой строки (последняя) и все незавершённые**. История не копится.

### Если merge упал

Упавший merge оставляет строку с пустым `finished_at`, её heartbeat со временем протухает. Такая строка перестаёт держать `boundary`, а `recover_abandoned()` помечает её как «изменилась» — чтобы витрины, пропустившие этот merge, проснулись:

```python
tracker.recover_abandoned()   # вызывайте в начале своего цикла
```

## Mart (витрины)

### Зачем

Витрина — это таблица, собранная из одной или нескольких исходных. Её нужно пересчитывать, когда источники меняются, и не трогать, когда они не менялись. `MartLoop` берёт на себя всю механику «когда запускать», оставляя вам только сам расчёт.

### Минимальный пример

```python
from dbmerge import dbmerge
from dbmerge.tracking import MergeTracker
from dbmerge.mart import Handler

class OrdersByCustomer(Handler):
    name = "orders_by_customer"             # уникальное имя витрины (для аренды)
    mart_table = "orders_by_customer"       # таблица-результат
    sources = ["orders", "order_items"]     # за чем следить
    min_interval = 5.0

    def setup(self, ctx):
        # одноразовый DDL: создать витрину
        with dbmerge(ctx.engine, table_name=self.mart_table, data=[],
                     key=["customer_id"]) as merge:
            merge.exec()

    def handle(self, ctx):
        # читаем всё, что изменилось после знака — без верхней границы: строки правее
        # ctx.boundary обработаются сейчас и повторно в следующем окне
        with dbmerge(ctx.engine, table_name=self.mart_table,
                     data=build_rows(since=ctx.last_run_at),
                     key=["customer_id"]) as merge:
            ctx.tracker.track(merge, boundary=ctx.boundary)

    def rebuild(self, ctx):
        # полная пересборка; по умолчанию — то же, что handle с full_rebuild=True
        self.handle(ctx)

tracker = MergeTracker(engine)             # один трекер на процесс
OrdersByCustomer().run_forever(engine, tracker)
```

Низкоуровневый вариант без класса — `MartLoop(engine, tracker, name=..., mart_table=..., sources=..., handle=handle)`, где `handle` — обычная функция. `Handler` — удобная обёртка над ним.

Важно: чтобы `MartLoop` видел изменения источников, их merge тоже нужно оборачивать в `tracker.track(...)` — как в разделе Tracking.

### Что делает MartLoop

Каждый обход (раз в `poll_interval` секунд):

1. Помечает протухшие merge (`recover_abandoned`).
2. Захватывает имя витрины — чтобы один mart не считали два процесса сразу.
3. Читает свой «водяной знак»: `last_run_at = tracker.watermark(mart_table)` (первый запуск — с самого начала).
4. Считает границу: `boundary = tracker.boundary(sources)`.
5. Спрашивает: `tracker.changed_since(sources, since=last_run_at)`.
   - нет изменений → пропуск;
   - изменения есть, но `boundary <= last_run_at` → ждём (источник ещё в процессе записи).
6. Иначе вызывает `handle(ctx)`.

Главное: **решение «обновлять ли» принимается по датам, а не по сигналу**. Событие, которое «потерялось» (процесс упал, витрина была выключена), никуда не денется — следующий обход увидит его через `changed_since`. Поэтому `handle` обязан быть **идемпотентным**: его могут выполнить повторно.

### Водяной знак

`last_run_at` хранить отдельно не нужно. Когда `handle` завершает merge витрины с параметром `boundary=ctx.boundary`, этот `boundary` записывается в tracking-строку витрины — следующий запуск начнёт ровно с него.

Чтение в обработчике — **без верхней границы**: выбирайте строки по `> ctx.last_run_at`. Строки правее `ctx.boundary` обработаются сразу (витрина свежее) и повторно в следующем окне (at-least-once). Ограничен только **знак**: он всегда равен `ctx.boundary`, иначе курсор перепрыгнул бы ещё не закоммиченные строки незавершённого merge.

«Пересобрать витрину с нуля» = удалить её завершённую строку:

```python
tracker.reset("orders_by_customer")   # следующий запуск пойдёт от начала
```

### Контекст обработчика

```python
@dataclass
class MartContext:
    engine: Engine
    tracker: MergeTracker
    name: str
    mart_table: str
    sources: frozenset[str]
    last_run_at: datetime      # откуда читать изменения
    boundary: datetime         # куда двигать водяной знак (не граница чтения)
    full_rebuild: bool         # True на первом прогоне / после reset
    lease_is_mine: Any         # SQL-условие «имя всё ещё наше»
```

## Схема таблицы

```
dbmerge_tracking
    id            String(64)  PK
    owner         String(255) NULL      -- NULL у lease-строк («свободно»)
    table_name    String(255) NOT NULL
    database_type String(64)  NULL      -- где живёт слитая таблица
    server        String(255) NULL
    database_name String(255) NULL
    started_at    DateTime(tz) NOT NULL
    finished_at   DateTime(tz) NULL
    heartbeat_at  DateTime(tz) NOT NULL
    changed       Boolean NULL
    boundary      DateTime(tz) NULL
```

## Справочник

### MergeTracker

| метод | что делает |
|---|---|
| `MergeTracker(engine, …)` | долгоживущий журнал: один трекер обслуживает много merge. Опционально `with MergeTracker(engine) as tracker:` останавливает heartbeat при выходе |
| `track(merge, *, boundary=None, changed=None, **exec_kwargs)` | пишет tracking-строку, вызывает `merge.exec(**exec_kwargs)`, завершает строку; возвращает `(MergeRecord, mergeResult)` |
| `boundary(table_names, database=None)` | безопасная верхняя граница по таблицам |
| `last_merge(table_name, database=None)` | последняя завершённая строка или `None` |
| `watermark(table_name, database=None)` | граница последнего успешного merge витрины или `None` |
| `changed_since(table_names, since, database=None)` | были ли реальные изменения после `since` |
| `recover_abandoned()` | протухшие незавершённые → пометить как «изменилась» |
| `reset(table_name)` | удалить завершённую строку (пересборка с нуля) |

`database_of(engine)` возвращает `Database(type, server, database)` — идентификатор базы движка; его подставляют в `database=` методов чтения. Аренда имени витрины — класс `Lease` в той же таблице (ключ `lease:<name>`).

### MartLoop

| метод | что делает |
|---|---|
| `run_forever()` | блокирующий цикл опроса |
| `run_if_pending()` | один обход (для cron или своего цикла) |
| `request_stop()` / `close()` | остановка |

### Handler

Базовый класс витрины: объявите `name`, `mart_table`, `sources` и реализуйте методы.

| метод | что делает |
|---|---|
| `setup(ctx)` | одноразовая подготовка перед первым прогоном (DDL) |
| `handle(ctx)` | инкрементальная работа; записывает знак через `tracker.track(..., boundary=ctx.boundary)` |
| `rebuild(ctx)` | полная пересборка; по умолчанию — `handle` с `full_rebuild=True` |
| `changed_since(ctx, *columns)` | OR-условие `column > ctx.last_run_at` |
| `execute(ctx, sql, **params)` / `query(ctx, sql, **params)` | выполнить SQL в своей транзакции / вернуть строки |
| `loop(engine, tracker, ...)` | собрать `MartLoop` |
| `run_forever(engine, tracker, ...)` | запустить цикл |

`as_handler(obj)` приводит экземпляр `Handler`, модуль или функцию с атрибутами к `Handler`.

## Ограничения, о которых стоит помнить

- **Idempotency.** `handle` может выполниться повторно — данные не должны задваиваться.
- **Latency.** Витрина обновляется с задержкой до `poll_interval` (это опрос, не push).
- **`merged_on_field`.** Исходные таблицы мержатся с колонкой времени изменения, а обработчик выбирает строки по `> ctx.last_run_at` — иначе нечего будет «догонять» по окну.
- **Один процесс на витрину.** Имя витрины захватывается арендой; второй процесс пропускает обход.
