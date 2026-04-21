# Моделирование Data Warehouse и ETL-скрипты

Проект по проектированию хранилища данных и разработке ETL-процессов для загрузки данных в витрины.

## Задача

Спроектировать схему Data Warehouse (DWH) по методологии Kimball (звёздочка/снежинка) и написать ETL-скрипты для загрузки данных из источника в витрины.

## Технологии

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=postgresql&logoColor=white)

## Что сделано

- Спроектированы таблицы фактов и измерений для витрины продаж
- Написаны ETL-скрипты на Python для инкрементальной загрузки данных
- Реализованы SCD Type 2 (медленно меняющиеся измерения)
- Выполнена денормализация данных для ускорения аналитических запросов

## Результат

- Витрина данных обновляется по расписанию с контролем дублей
- Скрипты обрабатывают только новые и изменённые записи
- Схема DWH оптимизирована для типовых отчётов (продажи по продуктам, клиентам, датам)

## Как запустить

```bash
# Клонировать репозиторий
git clone https://github.com/unbelievablea/dwh-modeling-sales.git
cd dwh-modeling-sales

# Запустить скрипты в порядке нумерации
python scripts/01_create_tables.py
python scripts/02_load_dimensions.py
python scripts/03_load_facts.py
