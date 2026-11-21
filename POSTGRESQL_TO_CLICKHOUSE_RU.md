# Загрузка данных из PostgreSQL в ClickHouse

Этот способ подходит, если сервер ClickHouse может напрямую подключаться к PostgreSQL. Python запускает merge через подключение к ClickHouse, а ClickHouse загружает данные из PostgreSQL в staging. Строки не проходят через Python. Источник читается при каждом запуске; это не непрерывная репликация.

## 1. Подключите PostgreSQL к ClickHouse

ClickHouse должен иметь сетевой доступ к PostgreSQL. Используйте учётную запись с правом чтения источника. В примере база PostgreSQL `shop` содержит таблицу `public.facts` со столбцами `shop`, `product`, `day` и `qty`; ключ `(shop, product, day)` уникален. Замените параметры подключения и выполните в ClickHouse:

```sql
CREATE DATABASE shop_pg
ENGINE = PostgreSQL('pg-host:5432', 'shop', 'reader', 'replace-me', 'public');

SELECT shop, product, day, qty FROM shop_pg.facts LIMIT 5;
```

## 2. Выполните merge

Установите `dbmerge[clickhouse]`. Из Python подключайтесь **только к ClickHouse**. Пример пишет в `default.Facts` и сохраняет строки, которых нет в источнике. DBMerge может создать целевую таблицу с типами источника. Если создаёте её сами, используйте `ReplacingMergeTree` с `ORDER BY (shop, product, day)` и ключевыми столбцами без `Nullable`.

```python
from sqlalchemy import create_engine
from dbmerge import DBMerge

engine = create_engine("clickhousedb://default:@localhost:8123/default")
with DBMerge(
    engine=engine,
    source_schema="shop_pg",
    source_table_name="facts",
    schema="default",
    table_name="Facts",
    key=["shop", "product", "day"],
) as merge:
    result = merge.exec()

print(result.inserted_row_count, result.updated_row_count)
```

`source_schema` — имя подключённой **базы ClickHouse** (`shop_pg`), а не схемы PostgreSQL (`public`). Источником может быть и представление в подключённой схеме. Для чтения актуальных версий строк используйте `FINAL`:

```sql
SELECT shop, product, day, qty FROM default.Facts FINAL;
```

## 3. Выберите область обновления

- **Только новые и изменённые строки:** оставьте `delete_mode='no'`. Если источник хранит сведения об изменениях, выбирайте нужные строки через `source_condition` на `merge.source_table`.
- **Полный снимок месяца или магазина:** используйте [рецепт для одного среза](GUIDES.md#scoped-snapshot). Перед включением удаления или пометки задайте согласованные фильтры `source_condition` и `delete_condition`.
- **Полная копия:** используйте [рецепт полного снимка](GUIDES.md#full-snapshot), только если PostgreSQL возвращает все строки, которые должны остаться в цели.

ClickHouse может передать простые сравнения в PostgreSQL, но более сложные условия может применить уже после чтения. Подробнее — в [справке по источникам ClickHouse](DOCUMENTATION.md#source-tables-on-clickhouse).

[EN](POSTGRESQL_TO_CLICKHOUSE.md)