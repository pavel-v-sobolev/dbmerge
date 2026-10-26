# Исправления по аудиту 2.0.0: статус

**Дата:** 2026-10-26. Относится к [плану задач](dbmerge-2.0.0-review-tasks.md) и
[перепроверке](dbmerge-2.0.0-recheck.md). Каждое замечание сначала воспроизведено на живых серверах
(PostgreSQL ×3 драйвера, CockroachDB ×2, MS SQL pyodbc / mssql-python, SQLite, DuckDB, MySQL, MariaDB,
ClickHouse; SQLAlchemy 2.0.54 и 2.1.6), затем исправлено и закреплено тестом, который на старом коде падает.

## Исправлено

| ID | Что было | Что сделано | Тест |
|---|---|---|---|
| 1а | Ошибка COPY подменялась `current transaction is aborted` (PostgreSQL ×3, CockroachDB ×2, оба `commit_all_steps`); с naive-`datetime` приходила «сырая» ошибка драйвера вместо `DataError` | Патч аудита: транзакция до COPY, ошибка драйвера оборачивается `DBAPIError.instance` | `test_bulk_load_reports_the_original_error` — расширен на все базы с bulk-загрузкой, naive-`datetime` и оба `commit_all_steps` |
| 1б | Только SQLAlchemy 2.1 + psycopg 3: JSON без bind processor; с pyarrow пачка молча шла построчно, без pyarrow мерж падал | Патч аудита: сериализация JSON самим dbmerge по правилам SQLAlchemy | `test_json_goes_through_copy` — шпион на `_copy`, сравнение с построчной вставкой, JSON null / SQL NULL, оба `none_as_null`, с pyarrow и без |
| T-04 | `_assert_same_rows` не сравнивал длины | Сравниваются число строк и набор колонок | все тесты паритета |
| DB-08 | Ключи `list[dict]` из первой строки: лишний ключ терялся; недостающий падал построчно, но **при COPY молча становился NULL** | Все строки обязаны иметь ключи первой: `IncorrectDataError` с номером строки (0.14 с на 1 млн строк) | `test_list_of_dict_rows_with_other_keys_are_refused` |
| DB-09 | Дубль колонки pandas: побеждало второе значение | `IncorrectDataError` | `test_pandas_duplicate_columns_are_refused` |
| DB-07 | Генерируемая колонка цели попадала в UPDATE при `source_table_name` | Из источника пропускается (цель считает сама), в `data` — `IncorrectParameter` до записи. DuckDB не отличает generated от DEFAULT в каталоге — там отказывает сама база | `test_computed_target_column_is_not_written` |
| DB-12 | Сбой очистки подменял исходную ошибку, соединение не закрывалось | `_close()`: никогда не бросает, повторяет DROP на свежем соединении, затем предупреждение с именем таблицы (решение автора); соединение закрывается всегда; откат в `exec()` тоже не подменяет ошибку | `test_failed_cleanup_keeps_the_error_and_closes` |
| 5а | **Только pyodbc**: ошибка в конце пачки → ложная `IntegrityError` по PK staging | Каждая пачка в savepoint, откат к нему перед построчной вставкой. Цена ~0.2 с на 1 млн строк (11.24 против 11.04 с) | входит в `test_bulk_load_reports_the_original_error` |
| 5б | mssql-python: отдельное подключение на каждый bulk copy (~40 мс) | Пачка 100 000 по умолчанию, если `chunk_size` не задан: 1 млн строк 3.4 → 1.7 с | замер |
| — | mssql-python: курсор без `close()` | `try/finally` | — |
| DB-13, DB-06, N-05 | Документация шире реализации | Уточнены `total_time`, `*_sql`, ограничение UPDATE → DELETE при `UNIQUE`, «never NULL» у `delete_mark_field` | — |
| Задача 3 (часть) | Исключения не экспортировались из корня | Экспортируются; старый путь работает | `test_exceptions_are_exported_from_the_package_root` |
| Задача 4 | Нет CI для Python 3.10–3.12 и без pyarrow | Workflow `test_python.yml`: 3.10 без pyarrow, 3.12 без pyarrow, 3.15 с pyarrow | профили прогнаны локально, все зелёные |
| Задача 2 | Нет `CHANGELOG.md` | `CHANGELOG.md` (Keep a Changelog: 2.0.1, 2.0.0 с «Upgrading from 1.x»), в sdist, ссылка `Changelog` в `pyproject.toml`; версия 2.0.1; wheel и sdist собраны и проверены | — |
| — | ClickHouse: неверное значение давало «сырой» `ArrowInvalid` без колонки | `IncorrectDataError` с колонкой и сквозным номером строки. Штатного способа узнать позицию в pyarrow нет (имя колонки он даёт только для pandas), поэтому строка ищется делением пополам, только при ошибке | `test_clickhouse_reports_column_and_row_of_a_bad_value` |

## Найдено при исправлении

- **DuckDB:** пачку, которую Arrow не может собрать (колонка смешанных типов), загрузчик отдавал «сырым»
  `pyarrow.ArrowInvalid`. Теперь такая пачка идёт построчно, и ошибку сообщает DuckDB.
- **COPY без pyarrow:** модуль `csv` писал `1.0` в целочисленную колонку (pandas хранит int с пропуском
  как float), COPY отклонял. Исправлено в ветке `csv`. Найдено профилем Python 3.12 без pyarrow.
- **Тесты без pyarrow:** `pl.from_pandas` требует pyarrow — добавлен `_to_polars`.
- **scramp 1.5.0** (2026-10-25) ломает pg8000 1.31.5 — внешнее, описано в `COMPATIBILITY.md`, обход `scramp<1.5`.
- **sdist:** шаблоны `include` в hatch работают как в `.gitignore`, и `"README.md"` захватывал README из `audit/` и `tests/ai_audit/`. Шаблоны привязаны к корню.
- **Тесты на MariaDB:** движки тестов не освобождались, и полный набор исчерпывал 151 подключение (`Too many connections`). Теперь их освобождает autouse-фикстура.

## Не исправлено — по решению автора

- **CockroachDB: COPY не применяет ограничения типа колонки** (24.3 и 26.3): строка длиннее `VARCHAR(n)`/`CHAR(n)`
  обрезается, число сверх `DECIMAL(p,s)` сохраняется, лишние знаки не округляются; построчная вставка
  отклоняет/округляет. `INSERT … SELECT` проверки делает, так что двухшаговая загрузка была бы точной,
  но автор решил не усложнять ради CockroachDB: описано в `COMPATIBILITY.md` и `DOCUMENTATION.md`
  (обход — `bulk_load=False`).
- Задача 6: исходные отчёты 1.0.20 автор удалил из `tests/ai_audit/`; они остаются в истории git, коммит `6265294`.
- Псевдоним `DBMerge` не добавлен: второе имя того же класса не решает коллизию имён пакета и класса.

## Где аудит был неточен

- **5а для mssql-python** не воспроизводится: bulk copy одной пачкой атомарен, ошибка приходит правильная.
- **Задача 4, «uv не фиксирует Python»:** в логах CI везде `Python 3.13.17`; верно лишь, что 3.10–3.12 и путь
  без pyarrow не проверялись.
- **1а** серьёзнее, чем описано: с naive-`datetime` ошибка не подменялась, но приходила неверным классом.

## Регрессия

Полный `tests/test_all.py` на окончательном коде, 2026-10-26, Python 3.14.3, SQLAlchemy 2.0.54 и 2.1.6.
Каждая база и драйвер — отдельный набор (`DBMERGE_TEST_ENGINES`), пропуски — тесты других баз,
`source_table` на ClickHouse и mssql-python на SQLAlchemy 2.0, где его диалекта нет.

| База | Драйверы | SQLAlchemy 2.0 | SQLAlchemy 2.1 |
|---|---|---|---|
| PostgreSQL 18 | psycopg2, psycopg 3, pg8000 | 87 passed каждый | 87 passed каждый |
| CockroachDB 24.3 (свежий контейнер) | psycopg2 / psycopg 3 | 87 / 86 + 1 failed ¹ | 87 / 87 |
| MariaDB 10.11 | mariadb | 81 | 81 |
| MySQL 8.4 | PyMySQL, mysql-connector, mysqlclient | 81 каждый | 81 каждый |
| SQL Server 2022 | pyodbc / mssql-python / pymssql | 81 / — / 80 + 1 xfailed ² | 81 / 81 / 80 + 1 xfailed ² |
| SQLite, DuckDB | — | 80 каждый | 80 каждый |
| ClickHouse 23.8 / 24.8 / 26.9 | clickhouse-connect | 81 / 81 / 82 | 81 / 81 / 82 |

1. `SerializationFailure: restart transaction` — временная ошибка повтора транзакции CockroachDB, которую клиент
   должен повторить (описано в `COMPATIBILITY.md`, dbmerge повторов не делает). Тот же тест отдельно прошёл.
2. pymssql обрезает микросекунды до миллисекунд — ограничение драйвера, тест помечен strict xfail.

Без pyarrow (свежие пакеты, без `uv.lock`): Python 3.12 — SQLite + DuckDB 159 passed, PostgreSQL ×3 256 passed;
Python 3.10 — SQLite 80, PostgreSQL 81. Python 3.15 со всеми драйверами прогонялся до этих исправлений (2026-10-25).
