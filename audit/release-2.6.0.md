# Подготовка dbmerge 2.6.0 — 17 ноября 2026

Статус: **локальные артефакты подготовлены; публикация ещё не разрешена полным CI**.
Commit/push разрешены пользователем для серверной CI-приёмки. Tag, GitHub Release
и загрузка в PyPI этой задачей не выполняются.
Существующие пользовательские изменения рабочего дерева сохранены.

## Содержание релиза

- Канонический `DBMerge`, старое имя `dbmerge` сохранено как alias.
- Защита внешнего source SELECT от несвязанных FROM до staging INSERT.
- Первичная сборка витрины без change-сигнала при отсутствии watermark.
- Идемпотентная регистрация MartLoop и очистка owned-ресурсов при ошибке конструктора.
- Исправления длинных lease-ключей, конкурентного создания служебных таблиц,
  валидации интервалов и shutdown runner.
- Примеры `product_daily_sales` вместо прежних customer-total примеров:
  источник `Order`, `Date`, `Product`, `Shop`, `Amount`; бизнес-результат
  `Date`, `Product`, `SUM(Amount)` по всем магазинам. PostgreSQL/ClickHouse,
  journaled seeder, runner и rebuild script. Технические поля сохранены для
  инкрементов, tombstones, переноса групп и возобновления rebuild.
- Табличное описание всех полей tracking/marts и PyPI-совместимые README-ссылки.

## Изменения при обновлении

1. Остановить старые mart workers, убедиться в готовности источников, затем
   запускать новые. Назначения без watermark автоматически выполнят initial
   rebuild: заранее предусмотреть нагрузку. Регистрация не запускает расчёт.
2. В source_condition использовать source-колонки; target-фильтр удаления
   передавать в delete_condition. Несвязанный FROM теперь вызывает
   IncorrectParameter; связанные JOIN и подзапросы поддержаны.
3. `from dbmerge import dbmerge` остаётся совместимым. Каноническое имя класса
   и `DBMerge.__name__` — `DBMerge`: строковые проверки старого имени обновить.
   Имя пакета и logger по-прежнему `dbmerge`.
4. Примеры переименованы без compatibility wrappers. Обновить импорты на
   `examples.product_daily_sales.ProductDailySales` либо ClickHouse-вариант.
   Это новые demo-таблицы, не автоматическая миграция старых customer-данных.
5. Tracking/marts остаются beta; cross-engine запись и journal не атомарны.
   Handlers должны быть идемпотентны. Текущие delete/commit defaults не изменены.
6. Занятая аренда/таймаут теперь вызывают `LeaseBusy` из `DBMerge.exec()`,
   `MergeTracker.exec()` и `exec_merge()`, а не возвращают `None`/`(None, None)`.
   Для standalone retry/skip перехватывать исключение. MartLoop перехватывает
   его сам без продвижения незаписанного блока/watermark. Успешный no-op —
   обычный результат с нулевыми счётчиками, не ошибка. `Lease.acquire()` остаётся boolean.

## Проверки текущей подготовки

Результаты пересекаются, их нельзя суммировать. Skips не являются покрытием.

| Проверка | Результат | Журнал |
|---|---|---|
| SQLite, API/validation/startup/leases/docs/tracking/marts/source | 277 passed, 3 skipped | `/tmp/dbmerge-260-regression.log` |
| PostgreSQL, tracking/marts/source/concurrency/docs, SQLAlchemy 2.1, после упрощений и LeaseBusy API | 215 passed, 10 skipped, exit 0 | `/tmp/dbmerge-260-pg-final.log` |
| PostgreSQL, новый пример/concurrency, SQLAlchemy 2.0 | 14 passed, 7 skipped, exit 0 | `/tmp/dbmerge-260-pg20-final.log` |
| ClickHouse 25.8 + PostgreSQL, marts/source и новый shipped handler | 22 passed, exit 0 | `/tmp/dbmerge-260-clickhouse-final.log` |
| Финальные docs/runner | 19 passed, exit 0 | `/tmp/dbmerge-260-docs-final.log` |
| SQLite, tracking/marts/docs/runner после LeaseBusy API | 187 passed, 1 skipped, exit 0 | `/tmp/dbmerge-260-last-regression.log` |
| Полный SQLite suite, промежуточный прогон | 1 failed, 362 passed, 64 skipped: старый scope-тест ещё ожидал None; тест обновлён, затем целевой набор прошёл | `/tmp/dbmerge-260-sqlite.log` |
| Полный SQLite suite после обновления scope-теста | 363 passed, 64 skipped, exit 0 | `/tmp/dbmerge-260-full-final.log`, `/tmp/dbmerge-260-full-final.exit` |
| uv lock --check --offline, diff whitespace, compileall | exit 0 | `/tmp/dbmerge-260-static.log` |
| sdist → wheel, twine, проверка состава архива, чистая установка Python 3.13, smoke | PASS, exit 0 | `/tmp/dbmerge-260-package-final.log` |

Первый ClickHouse-прогон не смог подключиться из контейнера к localhost-only
публикации PostgreSQL. Повторный прогон использовал внутренний адрес PostgreSQL
`172.17.0.2:5432` и прошёл. Этот адрес локального окружения не записан в примеры.

## Артефакты

Только текущая папка: `/tmp/dbmerge-release-2.6.0-final/`.
В старых папках остаются audit-сборки 2.5.0 и промежуточные сборки: не использовать.

| Файл | SHA-256 |
|---|---|
| `dbmerge-2.6.0-py3-none-any.whl` | `af9e7f48d98272674151ce518620e51f028b0bebba0c2caad37ebfb4b9e77866` |
| `dbmerge-2.6.0.tar.gz` | `b6b28e274d2e24ce7abe42ccaeb86d96064660fd6e312ecb4423593ec93c08d7` |

Версия согласована в pyproject.toml, uv.lock, changelog, README и llms.txt.
Wheel установлен вне checkout без optional dependencies; проверены metadata
2.6.0, оба имени класса, SQLite merge, initial mart rebuild и следующий idle pass.
В sdist присутствуют все три новых demo-файла и отсутствуют прежние имена;
в wheel присутствуют новые validation-модули.

## Чек-лист выпуска

Первый push: `ced0239`. Серверный CI выявил исчерпание PostgreSQL connection pool
на ClickHouse 23.8 из-за многократного раскрытия вложенного changed-groups SELECT.
Исправление: один раз прочитать ключи затронутых групп перед merge; агрегаты
по-прежнему загружаются серверным SELECT. Live 23.8: **3 passed**,
`/tmp/dbmerge-260-ch238-fixed.log`. Latest до исправления также проходил;
повторная проверка последует на исправленном коммите. Архивы выше пересобраны
после исправления примера; перед публикацией пересобрать из точного tag.

Также CI обнаружил зависимость SQLite rebuild-теста от оставшейся общей
`dbmerge_marts`: UUID-журналы не изолируют watermark/cursor. Fixture теперь
очищает mart-state вместе с бизнес-таблицами. Локальный tracking/marts:
**168 passed, 1 skipped**, `/tmp/dbmerge-260-sqlite-isolated.log`.

- [x] Заменить примеры, ссылки, импорты, runner и связанные тесты.
- [x] Проверить свёртку магазинов, разные даты, обновления, удаления и rebuild.
- [x] Подготовить версию 2.6.0, lock, changelog и migration notes.
- [x] Собрать и установить финальный wheel в чистое окружение.
- [x] Подтвердить завершение окончательного полного SQLite suite и записать результат.
- [x] Просмотреть итоговый diff, включая ранее существовавшие изменения.
- [ ] Прогнать полный GitHub server CI на коммите релиза: PostgreSQL,
      CockroachDB, MySQL/MariaDB, SQL Server, Oracle, ClickHouse, DuckDB,
      Python/SQLAlchemy matrix. Локальные частичные прогоны не заменяют его.
- [ ] Выполнить commit/push для CI; после зелёного CI отдельно согласовать tag и публикацию.
      Не использовать `dist/*` из старой общей папки; публиковать только
      проверенные versioned артефакты либо пересобрать их из точного release tag.
- [ ] После успешной публикации отдельно обновить onecdc.

Живая гонка CREATE SCHEMA на всех СУБД, performance baseline, CVE/fuzzing,
все remote URL/anchors не доказаны этой подготовкой. Исходные аудиты сохранены
как исторические отчёты; их старые версии и результаты не подменены новыми.