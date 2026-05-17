# BigDataSpark

Лабораторная работа N2: ETL на Apache Spark с формированием отчетных витрин в ClickHouse.


- PostgreSQL хранит исходную таблицу `mock_data` и построенную Spark модель "звезда".
- Apache Spark выполняет ETL-преобразования.
- ClickHouse хранит 6 итоговых отчетных таблиц.
- Все сервисы запускаются через Docker Compose.


## Архитектура решения

Поток данных:

```text
CSV files -> PostgreSQL.mock_data -> Spark -> PostgreSQL star schema -> Spark -> ClickHouse reports
```

В работе используется результат предыдущей лабораторной по преобразованию данных в аналитическую модель. В этой версии модель пересобирается Spark-джобой в виде звезды, чтобы выполнить требование текущего задания.

Таблицы звезды в PostgreSQL:

- `dim_customer` - клиенты, контакты, страна, питомец.
- `dim_seller` - продавцы.
- `dim_product` - товары, категории, бренд, параметры, рейтинг.
- `dim_store` - магазины и география магазинов.
- `dim_supplier` - поставщики и их география.
- `fact_sales` - факт продаж со ссылками на измерения.

Отчетные таблицы в ClickHouse:

- `report_product_sales` - продажи по продуктам: выручка, количество, рейтинг, отзывы, ранг продаж.
- `report_customer_sales` - продажи по клиентам: сумма покупок, средний чек, распределение по странам.
- `report_time_sales` - продажи по времени: месячные и годовые показатели через поля `sale_year`, `sale_month`.
- `report_store_sales` - продажи по магазинам: выручка, количество, средний чек, город и страна.
- `report_supplier_sales` - продажи по поставщикам: выручка, средняя цена товаров, страна поставщика.
- `report_product_quality` - качество продукции: рейтинги, отзывы, продажи и корреляция рейтинга с объемом продаж.

## Требования

Нужно установить:

- Docker
- Docker Compose

Локально устанавливать PostgreSQL, ClickHouse или Spark не требуется.

## Запуск

```bash
cd BigDataSpark
docker compose up -d
```

При первом запуске PostgreSQL автоматически создаст таблицу `mock_data` и загрузит 10 CSV-файлов из папки `data`. Всего должно быть загружено 10000 строк.

Проверка исходной загрузки:

```bash
docker compose exec postgres psql -U bigdata -d bigdata -c "select count(*) from mock_data;"
```


## Запуск Spark-джоб

Сбор модели звезды в PostgreSQL:

```bash
docker compose exec spark /opt/spark/bin/spark-submit --repositories https://repo1.maven.org/maven2 --packages org.postgresql:postgresql:42.7.3 /opt/spark/app/spark_star_to_postgres.py
```

Создание отчетных таблиц в ClickHouse:

```bash
docker compose exec spark /opt/spark/bin/spark-submit --packages org.postgresql:postgresql:42.7.3 /opt/spark/app/spark_reports_to_clickhouse.py
```

## Проверка результата

Проверить таблицы звезды в PostgreSQL:

```bash
docker compose exec postgres psql -U bigdata -d bigdata -c "select count(*) from fact_sales;"
```

Проверить список отчетных таблиц в ClickHouse:

```bash
docker compose exec clickhouse clickhouse-client --user bigdata --password bigdata --query "show tables"
```

Проверить количество строк в отчетах:

```bash
docker compose exec clickhouse clickhouse-client --user bigdata --password bigdata --query "
select * from (
select 'report_product_sales' as table_name, count() as rows from report_product_sales
union all select 'report_customer_sales', count() from report_customer_sales
union all select 'report_time_sales', count() from report_time_sales
union all select 'report_store_sales', count() from report_store_sales
union all select 'report_supplier_sales', count() from report_supplier_sales
union all select 'report_product_quality', count() from report_product_quality
)
order by table_name
"
```

Примеры аналитических запросов:

```bash
docker compose exec clickhouse clickhouse-client --user bigdata --password bigdata --query "
select product_name, product_category, quantity_sold, revenue
from report_product_sales
order by sales_rank
limit 10
"
```

```bash
docker compose exec clickhouse clickhouse-client --user bigdata --password bigdata --query "
select customer_first_name, customer_last_name, customer_country, total_spent, avg_check
from report_customer_sales
order by customer_rank
limit 10
"
```

```bash
docker compose exec clickhouse clickhouse-client --user bigdata --password bigdata --query "
select sale_year, sale_month, revenue, orders_count, avg_order_amount
from report_time_sales
order by sale_year, sale_month
limit 20
"
```

## Отчет о проделанной работе

1. Подготовлен Docker Compose с тремя обязательными сервисами:
   PostgreSQL, ClickHouse и Spark.

2. Реализована автоматическая загрузка исходных CSV-файлов в PostgreSQL:
   скрипт `postgres/init/01_mock_table.sql` создает таблицу `mock_data` и выполняет `COPY` для всех 10 файлов.

3. Реализован Spark ETL `app/spark_star_to_postgres.py`:
   джоба читает `mock_data` из PostgreSQL через JDBC, выделяет измерения клиентов, продавцов, товаров, магазинов и поставщиков, затем формирует таблицу фактов продаж.

4. Реализован Spark ETL `app/spark_reports_to_clickhouse.py`:
   джоба читает таблицы звезды из PostgreSQL, строит 6 витрин и пересоздает соответствующие таблицы в ClickHouse.

5. Для ClickHouse выбраны таблицы `MergeTree`, потому что они подходят для аналитических витрин и быстрых SQL-запросов по отчетным данным.

6. Реализованы все отчеты из задания:
   продажи по продуктам, клиентам, времени, магазинам, поставщикам и качеству продукции.

## Подключения

PostgreSQL:

- Host: `localhost`
- Port: `5432`
- Database: `bigdata`
- User: `bigdata`
- Password: `bigdata`

ClickHouse HTTP:

- Host: `localhost`
- Port: `8123`
- User: `bigdata`
- Password: `bigdata`

ClickHouse Native:

- Host: `localhost`
- Port: `9000`
- User: `bigdata`
- Password: `bigdata`
