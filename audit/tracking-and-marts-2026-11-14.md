# Аудит tracking и marts — 14 ноября 2026

## Область и итог

Проверены `src/dbmerge/tracking.py`, `src/dbmerge/mart.py`, `src/dbmerge/lease.py`,
публичные экспорты, примеры, руководство и тесты. Анализ основан на коде репозитория,
локальных воспроизведениях и тестировании, а не на предположении об exactly-once.

**Итог:** API витрин упрощён, несколько ошибок изоляции и жизненного цикла исправлены.
Однако механизм пока нельзя считать гарантированно надёжной доставкой всех изменений:
есть P1-дефекты модели журнала и ограничения конкурентного выполнения/rebuild.
Идемпотентность обработчика необходима, но сама по себе не устраняет потерю сигналов.

Приоритеты: **P1** — риск пропуска изменений или некорректной записи;
**P2** — восстановление/совместимость/эксплуатация; **P3** — удобство и наблюдаемость.

## Выполненные изменения

- Обязательный `engine` в конструкторе `MartHandler`.
- Обязательные `destination` и непустой `sources`: объявление в классе или переопределение
  в конструкторе; нормализация ссылок и ранняя валидация. `sources` неизменяемый.
- Убраны отдельный пользовательский `name` и `mart_table`; контекст содержит `destination`.
  Строка/пара `(schema, identifier)` обозначает логическую цель, не обязательно SQL-таблицу.
- Автоматический ограниченный по длине lease-key из нормализованного destination;
  уникальный owner для каждого автоматически созданного lease, даже при общем tracker.
- Источники группируются по схеме, boundary — минимум по группам; пустая схема фильтруется
  точно, а не означает «все схемы».
- Очистка finished rows и `mark_watermark` изолированы по схеме; `reset` получил фильтр схемы.
- Rebuild state хранится через `tracker.engine`, а не через движок назначения.
- Исправлены rebuild helpers, обращавшиеся к отсутствующим `_mart_identity` и
  `_identity_values`. Новый API разделяет schema служебной таблицы и schema назначения.
- Recovery исключает lease rows и повторно проверяет срок heartbeat при UPDATE.
- `run_mart` закрывает собственный tracker, не закрывает переданный; loop не закрывает
  переданный shared lease. Stop до входа в `run_forever` больше не теряется.
- Оба примера вызывают `run_mart(..., run_forever=False)` прямо под
  `if __name__ == "__main__"`, без функции main, аргументов и сигналов. Tracker создаётся
  автоматически, кроме ClickHouse-примера с отдельным PostgreSQL-журналом. Runner безопасен при импорте.
- В PostgreSQL-примере исправлено неквалифицированное чтение `orders` при rebuild.

## Оставшиеся замечания

### P1-1. Последующий no-op стирает предыдущее реальное изменение

**Место:** `MergeTracker._finish`, `MergeTracker.is_changed_since`.

`_finish` удаляет прежние finished rows данной таблицы/схемы, оставляя только последнюю.
Если после изменившего данные merge выполнить no-op до опроса витрины, останется
`changed=False`. Витрина не узнает об изменениях даже после длительного простоя.

**Подтверждение:** локальный fake merge с `inserted_row_count=1` дал
`is_changed_since('source', datetime(2000, 1, 1)) == True`; тот же merge с нулевыми счётчиками
дал `False`. Между вызовами витрина не запускалась.

**Рекомендация:** хранить отдельно монотонный `last_changed_at` / change sequence для
identity `(schema, identifier)`, не обнуляемый no-op. Последний merge, последнее изменение
и watermark потребителя — разные сущности. Нужны миграция и тесты: несколько no-op после
изменения, два потребителя с разными watermark, восстановленный failed merge и параллельные
finish в обратном порядке.

### P1-2. Lease не является fencing и не гарантирует отсутствие двух писателей

**Место:** `Lease.acquire`, `Lease.heartbeat`, `MergeTracker.exec`, `MartLoop._run_rebuild`.

После долгой паузы owner может потерять lease, а затем продолжить запись. `exec` не проверяет
владение перед каждой записью. При rebuild проверка выполняется после выдачи label:
код блока уже исполнился. Heartbeat фильтрует owner, но не проверяет TTL, позволяя прежнему
owner обновить просроченный lease, если его ещё не перехватили. Повторный acquire с тем же
owner разрешён: один shared Lease / tracker не даёт эксклюзивность потокам одного owner.

**Рекомендация:** fencing token с монотонным поколением, проверяемым целевым хранилищем
или атомарным publish; проверять владение перед работой и продвижением cursor/watermark.
Добавить локальную нерекурсивную блокировку по identity для общего owner. Отдельный
тест: остановить heartbeat, дождаться takeover, возобновить первого писателя.

### P1-3. Boundary не является согласованным snapshot

**Место:** `safe_boundary`, `tracker.exec`, SQL из примеров.

Отдельная транзакция журнала не атомарна с target merge. Активный merge удерживает
watermark, но не запрещает handler читать его частично записанные данные. Примеры читают
`merged_on > watermark` без верхней границы и пересчитывают агрегаты из живой таблицы.
Секундная точность времени SQLite и строгие сравнения `>` / `<=` создают дополнительные
пограничные случаи. `safe_boundary` групп разных схем также читает не в одном snapshot.

**Рекомендация:** формально определить окно, точность часов, момент timestamp исходной
записи и semantics commit. Для строгой гарантии использовать согласованный snapshot /
CDC position / sequence. Одна лишь верхняя граница не исправляет агрегирование по
частично записанным источникам. Тесты должны покрывать один timestamp, долгую транзакцию,
частичный commit и запуск нового source merge между boundary и SELECT.

### P1-4. Rebuild не всегда эквивалентен полной синхронизации

**Место:** `_run_rebuild`, `_run_increment`, rebuild обоих примеров.

Примеры выполняют upsert существующих месячных групп, но не удаляют destination-группы,
которых больше нет в источнике. Изменение `order_date` / `customer_id` оставляет прежний
агрегат: changed-keys SELECT видит только новые значения. Hard delete не оставляет ключа
для инкремента. Чтение пустого источника не очищает destination.

Инкременты между блоками потенциально обновляют будущий месяц, после чего более поздний
rebuild-блок может перезаписать его устаревшим snapshot обработчика. Cursor и данные
фиксируются отдельно: блок должен быть безопасен для повторного выполнения. Раннее
создание watermark не означает готовность полной витрины downstream-потребителю.

**Рекомендация:** различать upsert refresh и full replacement. Для полного rebuild — staging
generation + атомарный publish/swap; ready/rebuilding status; фиксированный rebuild boundary.
Для изменяемых ключей — old/new keys либо tombstones. Проверить crash до/после cursor,
пустой источник, удаление последней группы, перенос строки в другой месяц, DAG витрин.

### P2-1. Первый запуск без tracked change пропускается

**Место:** `_run_if_pending`.

При отсутствии watermark первый rebuild зависит от `is_changed_since`. Уже заполненный
источник без записи журнала не запускает витрину автоматически. Это согласуется с текущим
тестом, но неожиданно для пользователя отладочного main.

**Рекомендация:** документировать bootstrap policy; добавить явный режим принудительного rebuild
и проверку состояния upstream. Пока можно вызвать `request_rebuild` для destination.

### P2-2. Идентификаторы не включают сервер/БД; namespace lease ограничен

Identity tracking/state — только `(schema, identifier)`. Две БД с одинаковой парой должны
использовать разные tracking tables. Новая схема lease-key решает неоднозначность
`name` у витрин, но не добавляет database identity. Merge lease по-прежнему строит
`schema.table` строковой конкатенацией. `Lease.ensure` использует `key[:64]` как primary key:
произвольные длинные имена могут конфликтовать. `lease:` — зарезервированный namespace.

**Рекомендация:** единая canonical identity + hash-key для всех lease; проверка длины и
зарезервированного префикса; при необходимости явный project/database namespace.

### P2-3. Конкурентное создание и изменение служебных таблиц

**Место:** `_ensure_table`, `_ensure_marts_table`, `_ensure_mart_row`, `request_rebuild`.

Проверка существования и INSERT/CREATE разделены: два первых процесса могут одновременно
увидеть отсутствие строки/таблицы. Destination lease защищает часть путей, но не первый
CREATE общего tracker/state и не внешний rebuild request. На схемах с nullable/empty-string
особенностями (в частности Oracle) равенство пустой schema служебных строк требует отдельной
проверки; локальная SQLite-проверка не подтверждает такую переносимость.

**Рекомендация:** миграции при deployment, безопасный dialect-aware upsert / ограниченный
retry после IntegrityError и повторное чтение. Единая нормализация пустой schema. Проверить
свежий одновременный startup на PostgreSQL, CockroachDB, MySQL/MariaDB, MSSQL и Oracle.

### P2-4. Lifecycle и управление остановкой требуют дальнейшего усиления

`request_stop` действует на зарегистрированные loops; глобальный stop до регистрации
ещё может быть пропущен. `close` внешнего потока может освободить lease, пока handler
продолжает работу; требуется дождаться завершения. Публичный shared lease не исключает
повторное владение одним ключом двумя loops с одинаковым owner.

`runner_marts` проверяет futures по порядку; при нескольких витринах ошибка позднего future
может не обнаружиться, пока первый бесконечный future не завершится. Нынешний пример
запускает одну витрину, поэтому этот риск относится к его расширению.

**Рекомендация:** общий stop token, join/drain lifecycle и контроль futures через
`wait(..., return_when=FIRST_EXCEPTION)`; ограничить retry/poll/backoff и DB statement timeout.

### P2-5. ClickHouse-пример требует интеграционного подтверждения

Он предполагает уже настроенный внешний `orders` в ClickHouse. SQL timestamp/boolean,
ReplacingMergeTree duplicates, чтение до фонового merge, точность Decimal и soft-delete
должны быть проверены на реальной связке PostgreSQL → ClickHouse. Исправление хранения
state в tracker подтверждено отдельными SQLite engine, но не заменяет этот end-to-end тест.
В описании примера стоит окончательно отделить внешний источник от места создания view.

**Рекомендация:** интеграционный тест с provisioning внешней таблицы, repeated updates,
deletes, crash/resume; явно описать eventual consistency и стратегию чтения `FINAL` /
versioned rows там, где это требуется.

### P3. Контракт, производительность и наблюдаемость

- Валидация сейчас покрывает конфигурацию ссылок, обязательный engine и основные интервалы,
  но не все callable signatures, NaN/inf, TTL/period и длины строк служебных колонок.
- На tracking нет явных составных индексов для identity/status/time; lease rows сохраняются
  постоянно. Нужны EXPLAIN и тесты длительной эксплуатации с большим числом identities.
- Нужны метрики lag, watermark, длительности block/merge, recovery и lease loss; логировать
  destination рядом с техническим hash-key. Уточнить API результата pass: skipped/running/
  throttled/rebuilding/completed вместо одного bool.
- Включить руководство tracking/marts и при необходимости примеры в sdist; сейчас явный
  список `pyproject.toml` их не включает. Для необратимых изменений API выбрать соответствующую
  версию релиза; совместный запуск старых и новых worker недопустим из-за смены lease-key.

## Проверки

- Целевые tests tracking/marts после упрощения точек входа: **48 passed** на SQLite,
  включая прямой запуск обоих файлов, результаты однократного прохода и освобождение
  loop/tracker при успехе и исключениях.
- Полный `pytest tests -q`, `DBMERGE_TEST_ENGINES=sqlite`, после штатной генерации
  `tests/sample_data_in_sqlite.py`: первый подготовленный запуск **127 passed, 37 skipped**;
  итоговый запуск после добавления тестов main — **129 passed, 37 skipped** за 171 секунду.
- Первый неподготовленный полный запуск: 24 ошибки `no such table: Facts_source`; после
  подготовки, совпадающей с CI, ошибок нет. Эти ошибки не были дефектами tracking/marts.
- `compileall` для пакета, примеров и изменённых тестов; `git diff --check`.
- Однократные точки входа обоих примеров проверены с подменой engine на SQLite.
- Ручное воспроизведение потери changed-сигнала после no-op.

Реальные PostgreSQL/ClickHouse и прочая multi-engine матрица в этой сессии не проверены.
Нельзя интерпретировать SQLite-проверки и mocks как подтверждение всех dialect guarantees.

## Рекомендуемая очередность

### Дополнение: CI и аварийные сценарии (14 ноября 2026)

Этот раздел фиксирует последующие изменения; результаты выше относятся к исходному
аудиту и не описывают итоговое состояние ветки.

- В девяти workflow добавлена независимая матрица `suite: merge/tracking` (для
  Python — отдельные include entries), timeout каждого job — 30 минут.
  Tracking/marts больше не стоят после `test_all.py` в одной команде.
  Сохранены прежние driver/SQLAlchemy/Python/server матрицы и triggers.
- В ClickHouse/PostgreSQL добавлен отдельный mart job: реальная внешняя PostgreSQL
  таблица, ClickHouse aggregate view и destination, PostgreSQL tracking/state.
  Проверяются первая загрузка, повторные изменения, soft-delete, idle pass,
  watermark и явный rebuild. Результаты читаются с `FINAL`.
- Добавлены аварийный выход процесса после commit и recovery; новый процесс
  возобновляет rebuild по сохранённым cursor/boundary. Истечение lease/heartbeat
  моделируется изменением timestamp, а не ожиданием production TTL.
- Добавлены retry heartbeat после ошибки и close timeout без преждевременного
  dispose пула (для tracker и lease), concurrent request перед rebuild-start
  UPDATE, миграция старой mart-state таблицы с сохранением cursor/request flag.
- Локально: SQLite + PostgreSQL (psycopg2/psycopg/pg8000) — **437 passed**;
  ClickHouse + PostgreSQL integration — **1 passed**. Проверены compileall,
  diff whitespace, YAML и структурные assertions девяти изменённых workflow.
- Устранено драйвер-зависимое предположение в тесте committed SQL: psycopg
  может возвращать rowcount=-1; тест проверяет сохранение фактического результата
  драйвера, changed и записанную строку, а не требует rowcount=1.

Это не подтверждение успешного удалённого CI и не проверка physical-delete,
fencing произвольных destination writes или убийства процесса внутри ClickHouse
merge. Матрица CI стала шире по числу jobs и потребляет больше runner time.

1. Отдельное durable last-change состояние и регрессии потери события (P1-1).
2. Формальный контракт boundary/commit/snapshot и fencing (P1-2, P1-3).
3. Полноценный rebuild generation/publish, ready status, delete/key-change semantics (P1-4).
4. Конкурентный startup, canonical identity, lifecycle и multi-engine CI (P2).
5. Метрики, индексы, эксплуатационные команды, упаковка и дополнительная валидация (P3).