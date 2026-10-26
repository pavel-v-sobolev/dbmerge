# dbmerge: задачи после ревью 2.0.0

План дальнейших исправлений для `pavel-v-sobolev/dbmerge`. Перепроверен 2026-10-26
на `62652949471c033df454053917d03ae5a1503ceb` (тег `2.0.0` + один коммит
с правкой `COMPATIBILITY.md`). Это список задач, а не указание автоматически
применить патч, менять API или выпускать релиз при чтении аудита.

Текущие доказательства и статусы старых замечаний:
[перепроверка 2.0.0](dbmerge-2.0.0-recheck.md).
У задачи 1 есть применимый кандидат патча; приёмка ещё требует усиления тестов
и прогонов серверных СУБД. Сначала задачи 1, 7 и 4, затем необходимые проверки
задачи 5; задача 2 завершает релиз. Задачи 3 и 6 не блокируют исправление ошибок.

## Контекст

**Результаты, заявленные в исходном ревью** (PostgreSQL 16, SQLAlchemy 2.1.6,
Python 3.13; логи этих запусков не приложены и здесь заново не подтверждены):
- полный `tests/test_all.py` на SQLite, PostgreSQL (psycopg2, psycopg 3, pg8000) и DuckDB: 375 passed, 4 skipped;
- 1 млн строк в PostgreSQL через psycopg2: первая загрузка 9.6 с против 28.8 с в 1.0.24, загрузка staging 4.7 с против 24.9 с;
- MS SQL, MySQL, MariaDB, CockroachDB и ClickHouse в исходном ревью не запускались.
  Настройки серверов нужно брать из соответствующих `.github/workflows/test_*.yml`;
  compose-файлы в `db/` есть не для всех этих СУБД.

**Решения автора: не менять и не предлагать.**
- `commit_all_steps=True` остаётся значением по умолчанию.
- README и позиционирование не трогать.
- Удаление на ClickHouse остаётся через lightweight `DELETE`, без перехода на `ReplacingMergeTree(ver, is_deleted)`: так сделано ради совместимости.

**Стиль проекта**
- Комментарии в коде пишутся на английском, полными фразами, и объясняют «почему».
- Тесты лежат в `tests/test_all.py` и параметризованы по словарю `engines`. Набор СУБД задаётся переменной окружения, например `DBMERGE_TEST_ENGINES=postgres,postgres-psycopg,postgres-pg8000`.
- Перед тестами нужна `data/sample_data.sqlite`. Если её нет, выполнить
  `mkdir -p data && python tests/sample_data_in_sqlite.py`. Скрипт пересоздаёт
  `Facts_source`; для перепроверки лучше отдельная копия репозитория и тестовые БД.
- Записывать фактические версии Python, SQLAlchemy, драйверов и серверов, число
  `passed/failed/skipped` и причины пропусков. `make_engine()` пропускает случай,
  если драйвер не установлен; зелёный запуск с такими `skip` не подтверждает поддержку.

---

## Задача 1. Две ошибки загрузки через COPY (bug, приоритет 1)

Кандидат исправления — `audit/dbmerge-2.0.1-fixes.patch`. Из корня репозитория:
`git apply --check audit/dbmerge-2.0.1-fixes.patch`, затем при выполнении этой задачи
`git apply audit/dbmerge-2.0.1-fixes.patch`. Он меняет только
`src/dbmerge/_bulk_load.py` и `tests/test_all.py`.

### 1а. Ошибка COPY подменяется сообщением «current transaction is aborted»

**Симптом.** Возьмём значение, которое COPY не принимает, например текст в колонке `BigInteger`:
- с `bulk_load=False` пользователь получает `sqlalchemy.exc.DataError: invalid input syntax for type bigint: "not a number"`;
- с `bulk_load=True` (это поведение по умолчанию) он получает `sqlalchemy.exc.InternalError: current transaction is aborted` (у pg8000 — `InterfaceError: in failed transaction block`).

В исходном ревью ошибка заявлена на psycopg2, psycopg 3 и pg8000. Исходное исключение
остаётся только в цепочке traceback, поэтому обработчик ожидаемого исключения не
срабатывает. Нельзя требовать именно `DataError` от всех драйверов: сравнивать нужно
с обычным INSERT того же драйвера. Перепроверка подтвердила отсутствие транзакции
SQLAlchemy в момент вызова COPY с помощью подмены `_copy`, без PostgreSQL-сервера.

**Причина.** После создания staging-таблицы выполняется `conn.commit()`, и SQLAlchemy считает, что транзакции нет. COPY идёт через «сырой» курсор драйвера (`_copy`), и SQLAlchemy не знает о транзакции, которую драйвер открыл неявно. Когда COPY падает, `self.conn.rollback()` в `exec()` ничего не делает. Затем `_drop_temp_table()` в `finally` выполняет `DROP` в прерванной транзакции, и это новое исключение заменяет исходное.

Для psycopg2/psycopg колонка `DateTime(timezone=False)` приводит к `SHOW TimeZone`
через SQLAlchemy, что заранее регистрирует транзакцию и скрывает этот конкретный
сценарий. **Для pg8000 исключение:** код сразу выбирает UTC, не выполняя `SHOW`.
Поэтому наличие такой колонки не защищает pg8000 от ошибки. Тестировать оба
`commit_all_steps`, с naive datetime и без него.

**Исправление (в патче)**
- В начале `_postgres_copy` открывается транзакция: `if not merge.conn.in_transaction(): merge.conn.begin()`.
- В `_copy` исключение драйвера оборачивается через `DBAPIError.instance(...)`. Тогда класс исключения (`DataError`, `IntegrityError` и т. д.) одинаков при `bulk_load=True` и `False`.

### 1б. psycopg 3 и колонки JSON/JSONB: зависимость от версии SQLAlchemy

**Симптомы из исходного ревью на SQLAlchemy 2.1.6**
- **С pyarrow.** Каждый чанк с JSON-колонкой молча уходит в построчную вставку. В логе остаётся только сообщение уровня DEBUG `Rows … could not be written for COPY (Unsupported Type:struct…)`. На 200 000 строк staging загружается 5.5 с вместо 0.8 с.
- **Без pyarrow (Python 3.12+).** Модуль `csv` записывает в CSV Python-repr словаря (`{'a': 1}`), и merge падает с `invalid input syntax for type json`.

**Уточнённая причина.** В SQLAlchemy 2.1.6 базовый JSON-процессор возвращает `None`,
если диалект поддерживает native JSON serialization и не задан `json_serializer`.
У psycopg этот флаг включён. `_json_processors` подставляет `lambda v: v`, и dict
попадает в CSV как dict. Это следует из исходников
[JSON.bind_processor](https://github.com/sqlalchemy/sqlalchemy/blob/rel_2_1_6/lib/sqlalchemy/sql/sqltypes.py#L2743)
и [диалекта psycopg](https://github.com/sqlalchemy/sqlalchemy/blob/rel_2_1_6/lib/sqlalchemy/dialects/postgresql/psycopg.py#L349).

На установленной SQLAlchemy **2.0.54** `JSONB().bind_processor()` возвращает функцию
для всех трёх PostgreSQL-драйверов; локальные CSV-проверки проходят и без патча.
Не распространять дефект на все версии SQLAlchemy 2.x. Точную нижнюю границу
затронутых версий и `cockroachdb+psycopg` ещё нужно проверить. Наличие собственного
`json_serializer` — отдельный сценарий, а не доказательство исправности default-пути.

**Исправление (в патче).** Когда процессора нет, `_json_processors` сериализует значение сам (`dialect._json_serializer` или `json.dumps`) по тем же правилам, что и SQLAlchemy:
- `None` становится JSON `null`, а при `none_as_null=True` — SQL NULL;
- `JSON.NULL` всегда становится JSON `null`;
- `sqlalchemy.null()` всегда становится SQL NULL.

Сравнение с построчной вставкой на psycopg 3 и psycopg2 при обоих `none_as_null`
заявлено в исходном ревью, но **не закреплено тестами приложенного патча**.
В приёмку добавить `JSON`, `JSONB`, оба `none_as_null`, `None`, `JSON.NULL`,
`sqlalchemy.null()` и пользовательский serializer, с Arrow и с модулем `csv`.
Различать SQL NULL и JSON null через `IS NULL` и текстовое/JSON-представление в БД.

### Новые тесты (в патче)

| Тест | Что проверяет |
|---|---|
| `test_bulk_load_reports_the_original_error` | при `bulk_load=True` выбрасывается то же исключение, что при `False`, и в его тексте есть исходная ошибка |
| `test_bulk_load_without_pyarrow_stores_what_row_by_row_stores` | путь, где CSV пишет модуль `csv` (pyarrow скрыт через monkeypatch). В CI pyarrow стоит всегда, поэтому раньше этот путь не проверялся |
| `test_json_goes_through_copy` | отсутствие сообщения `row by row` в логе; само использование COPY пока не проверяется |

Исторические числа из исходного ревью: 8 падений из 18 новых случаев до патча,
18 прошедших после; 30 passed в расширенном COPY-наборе на SQLAlchemy 2.0.55 и
2.1.6; полный набор 393 passed, 4 skipped. Они не являются результатом текущего
прогона и не означают, что JSON-дефект воспроизводился на обеих версиях SQLAlchemy.

**Ограничения этих тестов:** `test_json_goes_through_copy` проверяет только отсутствие
сообщения `row by row`, но не вызов `_copy` и не сохранённые данные. Без Arrow на
Python < 3.12 выбирается обычный INSERT, и тест может пройти без COPY. Нужны spy
на `_copy`, проверка содержимого и явный skip только там, где COPY не поддерживается.
Также исправить проверку количества строк в `_assert_same_rows` (задача 7).

### Что сделать

1. Проверить и применить патч из корня: `git apply audit/dbmerge-2.0.1-fixes.patch`.
   Дополнить тесты по замечаниям выше; до этого кандидат не считать готовым к релизу.
2. Прогнать тесты:
   ```bash
   DBMERGE_TEST_ENGINES=postgres,postgres-psycopg,postgres-pg8000 \
     python -m pytest tests/test_all.py -q -rs -k "original_error or without_pyarrow_stores or json_goes_through_copy"
   DBMERGE_TEST_ENGINES=sqlite,postgres,postgres-psycopg,postgres-pg8000,duckdb python -m pytest tests/test_all.py -q -rs
   ```
3. Обязательно прогнать на CockroachDB (`cockroachdb,cockroachdb-psycopg`), где я не проверял. Там те же COPY и `_json_processors`.
4. Не менять построчный путь и код загрузчиков MS SQL, DuckDB, ClickHouse в рамках
   этой задачи. Но `_json_processors` используется также DuckDB и MS SQL, поэтому
   его изменение требует проверки JSON на этих путях, даже если их функции не меняются.

**Готово, когда** усиленные тесты проходят на всех движках семейства PostgreSQL,
включая CockroachDB, проверены общие JSON-пути DuckDB/MS SQL, а полный набор
не стал хуже. Пропуски из-за отсутствующего драйвера не засчитываются как проверка.

### Файл патча

Патч хранится отдельно: [dbmerge-2.0.1-fixes.patch](dbmerge-2.0.1-fixes.patch).
Его текст здесь не дублируется, чтобы версии не расходились. В перепроверке
2026-10-26 исходный патч сохранён без изменений; применимость проверена через
`git apply --check audit/dbmerge-2.0.1-fixes.patch`.

---

## Задача 2. Подготовить 2.0.1 с заметками к релизу (после проверки исправлений)

В checkout нет `CHANGELOG.md`, а ссылка `Changelog` в `pyproject.toml` ведёт на
GitHub Releases. Состояние удалённых releases в этой перепроверке не устанавливалось:
исходное утверждение «раздел пуст» нельзя считать текущим фактом.

1. Поднять версию в `pyproject.toml` до `2.0.1`.
2. Создать `CHANGELOG.md` в корне в формате Keep a Changelog с разделами `2.0.1` и `2.0.0`. В `2.0.0` добавить подраздел «Upgrading from 1.x».
3. Добавить `CHANGELOG.md` в `[tool.hatch.build.targets.sdist] include`. Ссылку `Changelog` в `[project.urls]` направить на `CHANGELOG.md` (или оставить на releases, если они будут заполнены).
4. Проверить, требует ли изменение версии обновления записи проекта в `uv.lock`;
   проверить сборку wheel/sdist и состав архивов. После выполненных задач описывать
   только действительно вошедшие исправления.
5. Подготовить тексты GitHub Release для тегов `2.0.0` и `2.0.1`. Публиковать лишь
   в рамках отдельного запроса на релиз. Проверить существующие releases и теги,
   указать проверенный commit; для публикации существующих тегов использовать
   `gh release create <tag> --verify-tag --notes-file <file>`.

**Пункты для 2.0.0.** Каждый сверить с `git diff 1.0.24..2.0.0` и ничего не дописывать от себя.
- **Added**
  - DuckDB.
  - ClickHouse через clickhouse-connect (`pip install dbmerge[clickhouse]`); целевая таблица — `ReplacingMergeTree`.
  - Драйверы psycopg 3, pg8000, mssql-python (только SQLAlchemy 2.1), PyMySQL, mysql-connector-python, mysqlclient.
  - `COMPATIBILITY.md`.
  - Параметр `exec(bulk_load=…)`.
- **Changed**
  - Быстрая загрузка staging включена по умолчанию: COPY на PostgreSQL и CockroachDB,
    bulk copy на mssql-python, `fast_executemany` на pyodbc, Arrow на DuckDB.
    Выключается через `exec(bulk_load=False)`. **ClickHouse всегда загружает данные
    bulk-путём и этот флаг игнорирует**; при наличии pyarrow используется Arrow,
    иначе нативная вставка clickhouse-connect.
  - `chunk_size` по умолчанию зависит от СУБД: 10 000, на ClickHouse 1 000 000.
  - На MySQL staging теперь обычная таблица вместо `TEMPORARY`, поэтому после убитого процесса она может остаться (см. «The Staging Table» в DOCUMENTATION.md). На MariaDB staging остаётся `TEMPORARY`.
  - Отсутствие pyarrow меняет путь в зависимости от СУБД и Python: для PostgreSQL
    на Python 3.12+ остаётся COPY через `csv` без этого предупреждения; на более
    раннем Python используется обычный INSERT с предупреждением. DuckDB и
    ClickHouse также предупреждают при переходе на свой путь без Arrow.
  - На PostgreSQL float в `NUMERIC` без точности может сохраниться с другим scale (`1.0` против `1`); численно значения равны.
- **Fixed**
  - В diff 1.0.24..2.0.0 есть `_affected_rows`, `_execute_dml` и
    `INSERT_COUNT_AS_TEXT_DRIVERS`: обход потери rowcount при INSERT … SELECT для
    psycopg 3 и mysql-connector, чтение результата DML для DuckDB. Live-проверку
    конкретных драйверов указывать отдельно от подтверждения изменения в коде.
- **API**
  - Сигнатура `__init__` не изменилась.
  - У `exec()` появился `bulk_load`, а значение `chunk_size` по умолчанию стало `None` (выбирается по СУБД).

**Пункты для 2.0.1 (Fixed):** задачи 1а и 1б, сформулированные для пользователя.

---

## Задача 3. Псевдоним `DBMerge` и экспорт исключений (необязательное улучшение API)

**Зачем.**
1. `DBMerge` может дать более привычное имя класса. Текущее имя `dbmerge` само по
   себе не является функциональным дефектом и не блокирует релиз исправлений.
2. Сейчас у пакета, модуля и класса одно имя: пакет `dbmerge`, модуль `dbmerge/dbmerge.py`, класс `dbmerge`. `__init__.py` выполняет `from .dbmerge import dbmerge`, поэтому атрибут пакета `dbmerge.dbmerge` — это класс, а не модуль. Из-за этого:
   - `import dbmerge; dbmerge.dbmerge.IncorrectParameter` падает с `AttributeError`;
   - `import dbmerge._bulk_load` внутри функции перепривязывает локальное имя `dbmerge` к пакету, и `dbmerge(...)` падает с `'module' object is not callable`. На это пришлось наткнуться при написании теста в задаче 1.
3. Исключения (`IncorrectParameter`, `NoKeyError` и др.) не экспортируются из корня пакета, поэтому пользователю приходится писать `from dbmerge.dbmerge import IncorrectParameter`.

Псевдоним не устраняет коллизию пакета/модуля/класса, пока старый экспорт остаётся.
Пример с `import dbmerge._bulk_load` — обычное перепривязывание Python-имени, а не
отдельная поломка импортов. Экспорт исключений из корня лишь упрощает публичный API.

**Что сделать.**
1. В `src/dbmerge/dbmerge.py` сразу после класса `dbmerge` добавить:
   ```python
   # The CapWords name (PEP 8) of the same class: isinstance, subclassing and type hints work with either name.
   DBMerge = dbmerge
   ```
2. В `src/dbmerge/__init__.py` импортировать и добавить в `__all__`: `DBMerge`, `TableNotFoundError`, `NoKeyError`, `IncorrectDataError`, `IncorrectParameter`, `TempTableAlreadyExists`.
3. Добавить тест:
   ```python
   def test_public_names():
       import dbmerge as package
       from dbmerge import DBMerge, dbmerge, IncorrectParameter, NoKeyError
       from dbmerge.dbmerge import IncorrectParameter as old_path
       assert DBMerge is dbmerge
       assert IncorrectParameter is old_path
       assert set(['DBMerge', 'IncorrectParameter', 'NoKeyError']) <= set(package.__all__)
   ```
4. В `DOCUMENTATION.md` (в начало раздела «Class: dbmerge») и в `llms.txt` добавить одну фразу: «`DBMerge` is the same class as `dbmerge`; both names are supported. Exceptions can be imported from the package root.»

**Не делать.** Не переименовывать класс, не добавлять `DeprecationWarning` для `dbmerge`, README не трогать.

---

## Задача 4. CI для Python 3.10–3.12 и без pyarrow

В `pyproject.toml` заявлено `requires-python = ">=3.10"`. Основные workflows
вызывают `uv python install 3.13`, но затем `uv sync` без `--python`: установка
интерпретатора сама по себе не фиксирует выбор для окружения. Отдельный workflow
3.15 явно делает `uv venv --python 3.15`. Успешных логов CI здесь не проверяли.
`tests/env_old.sh` — лишь рецепт локальной установки, а не свидетельство её успеха.

1. Добавить workflow с `postgres:16` и явным `uv venv --python <version>` либо
   `uv sync --python <version>`. Печатать `python --version` и версии зависимостей.

   | Профиль | Что проверяет |
   |---|---|
   | Python 3.10, SQLAlchemy 2.0.23, Alembic 1.13.0, pandas 2.0.1, NumPy 1.25.0, pytest 7.1.1 | Минимальные runtime/pandas зависимости; SQLite и PostgreSQL |
   | Python 3.11, без pyarrow | Обычный INSERT вместо COPY и предупреждение |
   | Python 3.12, без pyarrow | Реальный COPY через `csv.QUOTE_NOTNULL` |
   | Python 3.13+, SQLAlchemy 2.0 и 2.1, с pyarrow | COPY/JSON и обратная совместимость |

2. Не устанавливать `.[dev]` в профилях без Arrow: extra напрямую тянет pyarrow.
   Для запуска `test_all.py` нужны также pandas, NumPy и polars. После установки:
   `python -c "import importlib.util; assert importlib.util.find_spec('pyarrow') is None"`.
   Это должна быть **успешная** проверка отсутствия, а не намеренно падающий шаг CI.
3. В минимальном профиле зафиксировать совместимые версии всех тестовых зависимостей.
   Не переносить `psycopg2==2.9` из `env_old.sh` без проверки сборки и системных
   библиотек; если нужна более новая версия драйвера, явно назвать профиль
   «минимальные runtime-зависимости», а не «все минимальные зависимости».
4. Вызовы `pl.from_pandas` в существующем suite могут требовать pyarrow для object/date
   колонок. Для настоящего no-Arrow профиля подготовить данные без этой конверсии
   или явно отделить неподдерживаемую конверсию тестового fixture. Не пропускать
   весь backend/формат и не маскировать ошибку dbmerge без воспроизведения.
5. Проверять выбор загрузчика, число вызовов COPY, содержимое таблиц и предупреждения,
   а не только отсутствие исключения. Отдельный smoke-test с одними обязательными
   зависимостями (`sqlalchemy`, `alembic`) должен импортировать пакет и выполнять
   SQLite merge из dict/list без pandas, polars и pyarrow; `test_all.py` для этого
   не подходит из-за импортов на верхнем уровне.

---

## Задача 5. MS SQL: проверить две гипотезы

Это гипотезы, которые в текущей перепроверке не проверялись на сервере. Сначала
воспроизвести на `db/mssql/docker-compose.yml`, менять код только при подтверждении.
Для `mssql-python` нужен отдельный профиль SQLAlchemy 2.1 с установленным драйвером;
`make_engine()` иначе пропустит проверку. Проверить оба `commit_all_steps`.

### 5а. Частичный сбой чанка маскирует исходную ошибку

`_pyodbc_fast_executemany` и `_mssql_python_bulkcopy` (`src/dbmerge/_bulk_load.py`) при любом исключении перезаливают весь чанк построчно (`merge._insert_rows_to_temp(begin, end)`). Если до ошибки часть строк чанка уже легла в staging, построчная вставка упрётся в PK staging-таблицы. Пользователь увидит нарушение PK вместо исходной ошибки. Комментарий в коде («can not double rows silently») этот случай признаёт, но сообщение получается ложным.
- **pyodbc.** Строки остаются в той же транзакции.
- **mssql-python.** Bulk copy идёт отдельным соединением и коммитится сам.

**Как проверить.** Взять чанк из ~1000 строк, где ошибка на стороне сервера возникает только в последней строке. Например, `data_types={'name': String(10)}` и строка длиннее 10 символов, или дата вне диапазона `DATETIME`. Сравнить исключение с `bulk_load=False` (по образцу `test_bulk_load_reports_the_original_error`) и посмотреть, что осталось в staging. Проверить на `mssql` (pyodbc) и `mssql-python`.

**Если подтвердится:**
- для pyodbc исследовать savepoint с откатом **до** fallback и проверять, допускает
  ли состояние транзакции продолжение; не обещать, что любая ошибка восстанавливается
  через `begin_nested()`;
- для mssql-python проверить `use_internal_transaction=True` и один batch на чанк
  для **обоих** методов, `bulkcopy` и `bulkcopy_arrow`. Нельзя считать весь вызов
  атомарным лишь по имени опции: в API транзакция описана на batch. При неоднозначном
  частичном результате сохранять исходную ошибку, а не безусловно повторять INSERT;
- добавить тест, который повторяет сценарий проверки.

Кроме того, `_mssql_python_bulkcopy` создаёт cursor без `finally: cursor.close()`.
Отсутствие явного закрытия видно в коде; утечку серверного ресурса отдельно не
подтверждали, поскольку соединение закрывается в `exec()`. Проверить закрытие
курсора при успехе и ошибке. Семантика batch и отдельное соединение видны в
[исходнике mssql-python](https://github.com/microsoft/mssql-python/blob/main/mssql_python/cursor.py)
(проверен 2026-10-26); для реализации фиксировать конкретную версию драйвера.

### 5б. Стоимость логина при bulk copy в mssql-python

В проверенном исходнике mssql-python методы `bulkcopy()` и `bulkcopy_arrow()`
создают отдельный `PyCoreConnection` на вызов. При миллионе строк и чанке 10 000
это до 100 bulk-вызовов, если все чанки идут этим путём. Реальное число логинов
и их стоимость нужно измерить на установленной версии. Issue
[microsoft/mssql-python#600](https://github.com/microsoft/mssql-python/issues/600)
описывает ошибку подключения bulkcopy; сам по себе он не является замером логинов.

**Как проверить.** Замерить загрузку 1 млн строк с `chunk_size` 10 000, 100 000
и 1 000 000, с Arrow и без, фиксируя `temp_insert_time` и wall time. У текущего
`tests/measure_perf.py` **нет** аргумента chunk_size: сначала добавить параметр
измерительного скрипта либо использовать отдельный harness с
`merge.exec(chunk_size=...)`. Различать время CSV/Arrow-конверсии, логина и отправки;
повторить замеры, проверить счётчики/данные и отсутствие fallback.

**Если разница заметная.** Завести для этого пути свой размер чанка по умолчанию по образцу `DEFAULT_CHUNK_SIZE` модуля ClickHouse и сохранить проверку `datetimeoffset` по чанкам. Обновить таблицу «Bulk Staging Load» в `COMPATIBILITY.md`. Без замера ничего не менять.

---

## Задача 6. Убрать отчёты AI-аудита из `tests/`

В `tests/ai_audit/` лежат исторические отчёты по 1.0.20. По настройкам sdist они
не входят в пакет. В текущей перепроверке добавлен [указатель статусов](https://github.com/pavel-v-sobolev/dbmerge/tree/6265294/tests/ai_audit),
оригиналы сохранены. DOCX с названием `risks_by_design` фактически содержит
развёрнутый отчёт о дефектах, аналогичный HTML; имя файла не описывает содержимое.
Возможный перенос — необязательная организационная задача. Если переносить,
выбрать единый каталог с существующим `audit/`, обновить ссылки и сохранить даты
и версию объекта; не выдавать архивные NO-GO и результаты security scan за текущие.

---

## Задача 7. Незакрытые замечания 1.0.20 и пробелы регрессии

Подробные статусы и воспроизведения: [перепроверка](dbmerge-2.0.0-recheck.md).

| Приоритет | ID | Что сделать и критерий приёмки |
|---|---|---|
| P1 | DB-08, DB-09 | Проверять ключи каждой строки `list[dict]` и уникальность названий колонок pandas до DDL/DML. Ошибка должна быть явной, без потери полей/значений; проверить все пути загрузки. |
| P1 | DB-07 | Разделить читаемые source-поля и записываемые target-поля для generated/computed колонок; в непредусмотренных случаях отказать явно до DML. Воспроизведено на SQLite, прежний отчёт также описывает PostgreSQL. |
| P1 | T-04 | В `_assert_same_rows` проверять `len(first) == len(second) == 3` и одинаковый набор колонок перед сравнением. Сейчас `zip` принимает пустой, укороченный и удлинённый второй результат. |
| P2 | DB-06 | Зафиксировать ограничение порядка UPDATE → INSERT → DELETE для UNIQUE-переходов; отдельно решить, нужен ли preflight. Не менять порядок автоматически: это затрагивает FK и выбранные границы commit. |
| P2 | DB-12 | Сохранить исходную ошибку при сбое cleanup, гарантировать close во внешнем finally; покрыть `exec()` и `__exit__`. Патч COPY устраняет один триггер, но не общий дефект. |
| P2 | DB-13 | Уточнить, что `total_time` — сумма времён фаз без DDL/commit/cleanup, а `*_sql` — представление statement, не точный SQL драйвера. Добавление новой wall-time метрики — отдельное решение API. |

Здесь P1 означает приоритет исправления потери данных/проверок, P2 — уточнение
контракта и устойчивости. Старые N-01, N-03/N-04, N-05 нельзя автоматически
реализовывать как багфиксы: текущая документация сохраняет защищённые строки,
управляет указанной flag-колонкой и разрешает входной mark «как есть».

---

## Идеи на потом (не делать без запроса автора)

- Режим SCD2 (история изменений с `valid_from`/`valid_to`), который часто просят в DWH.
