import os
import urllib.parse
import urllib.request

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "bigdata")
POSTGRES_USER = os.getenv("POSTGRES_USER", "bigdata")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "bigdata")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_HTTP_PORT = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "default")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "bigdata")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "bigdata")

POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_PROPS = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}


def execute_clickhouse(sql):
    payload = sql.encode("utf-8")
    url = clickhouse_url()
    try:
        with urllib.request.urlopen(url, payload, timeout=30) as response:
            response.read()
    except urllib.error.HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"ClickHouse SQL failed: {details}") from exc


def clickhouse_url():
    user = urllib.parse.quote(CLICKHOUSE_USER, safe="")
    password = urllib.parse.quote(CLICKHOUSE_PASSWORD, safe="")
    return (
        f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}/"
        f"?user={user}&password={password}"
    )


def recreate_clickhouse_table(table_name, ddl_columns, order_by):
    execute_clickhouse(f"DROP TABLE IF EXISTS {CLICKHOUSE_DB}.{table_name}")
    execute_clickhouse(
        f"""
        CREATE TABLE {CLICKHOUSE_DB}.{table_name}
        (
            {ddl_columns}
        )
        ENGINE = MergeTree
        ORDER BY ({order_by})
        """
    )


def write_clickhouse(df, table_name):
    ch_url = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:8123/{CLICKHOUSE_DB}"

    ch_props = {
        "user": CLICKHOUSE_USER,
        "password": CLICKHOUSE_PASSWORD,
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    }

    df.write.jdbc(ch_url, table_name, "append", properties=ch_props)

    print(f"Written ClickHouse table: {table_name}, rows={df.count()}")

def read_postgres_table(spark, table_name):
    return spark.read.jdbc(POSTGRES_URL, table_name, properties=POSTGRES_PROPS)


def main():
    spark = (
        SparkSession.builder.appName("BigDataSpark - ClickHouse reports")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    fact_sales = read_postgres_table(spark, "fact_sales")
    dim_customer = read_postgres_table(spark, "dim_customer")
    dim_product = read_postgres_table(spark, "dim_product")
    dim_store = read_postgres_table(spark, "dim_store")
    dim_supplier = read_postgres_table(spark, "dim_supplier")

    sales = (
        fact_sales.alias("f")
        .join(dim_product.alias("p"), "product_id", "left")
        .join(dim_customer.alias("c"), "customer_id", "left")
        .join(dim_store.alias("st"), "store_id", "left")
        .join(dim_supplier.alias("su"), "supplier_id", "left")
    )

    product_window = Window.orderBy(F.col("quantity_sold").desc(), F.col("revenue").desc())
    product_sales = (
        sales.groupBy(
            "product_id",
            "product_name",
            "product_category",
            "product_brand",
            "product_rating",
            "product_reviews",
        )
        .agg(
            F.sum("sale_quantity").cast("long").alias("quantity_sold"),
            F.round(F.sum("sale_total_price"), 2).alias("revenue"),
            F.countDistinct("sale_id").cast("long").alias("orders_count"),
            F.round(F.avg("sale_total_price"), 2).alias("avg_order_amount"),
        )
        .withColumn("sales_rank", F.row_number().over(product_window))
        .select(
            "product_id",
            "product_name",
            "product_category",
            "product_brand",
            "quantity_sold",
            "revenue",
            "orders_count",
            "avg_order_amount",
            F.col("product_rating").alias("avg_rating"),
            F.col("product_reviews").alias("reviews_count"),
            "sales_rank",
        )
    )

    customer_sales_base = (
        sales.groupBy(
            "customer_id",
            "customer_first_name",
            "customer_last_name",
            "customer_email",
            "customer_country",
        )
        .agg(
            F.round(F.sum("sale_total_price"), 2).alias("total_spent"),
            F.countDistinct("sale_id").cast("long").alias("orders_count"),
            F.round(F.avg("sale_total_price"), 2).alias("avg_check"),
        )
    )
    country_distribution = (
        dim_customer.groupBy("customer_country")
        .agg(F.countDistinct("customer_id").cast("long").alias("customers_in_country"))
    )
    customer_window = Window.orderBy(F.col("total_spent").desc())
    customer_sales = (
        customer_sales_base.join(country_distribution, "customer_country", "left")
        .withColumn("customer_rank", F.row_number().over(customer_window))
        .select(
            "customer_id",
            "customer_first_name",
            "customer_last_name",
            "customer_email",
            "customer_country",
            "customers_in_country",
            "total_spent",
            "orders_count",
            "avg_check",
            "customer_rank",
        )
    )

    time_sales = (
        sales.withColumn("sale_year", F.year("sale_date"))
        .withColumn("sale_month", F.month("sale_date"))
        .groupBy("sale_year", "sale_month")
        .agg(
            F.round(F.sum("sale_total_price"), 2).alias("revenue"),
            F.sum("sale_quantity").cast("long").alias("quantity_sold"),
            F.countDistinct("sale_id").cast("long").alias("orders_count"),
            F.round(F.avg("sale_total_price"), 2).alias("avg_order_amount"),
        )
        .orderBy("sale_year", "sale_month")
    )

    store_window = Window.orderBy(F.col("revenue").desc())
    store_sales = (
        sales.groupBy(
            "store_id",
            "store_name",
            "store_city",
            "store_state",
            "store_country",
        )
        .agg(
            F.round(F.sum("sale_total_price"), 2).alias("revenue"),
            F.sum("sale_quantity").cast("long").alias("quantity_sold"),
            F.countDistinct("sale_id").cast("long").alias("orders_count"),
            F.round(F.avg("sale_total_price"), 2).alias("avg_check"),
        )
        .withColumn("store_rank", F.row_number().over(store_window))
        .select(
            "store_id",
            "store_name",
            "store_city",
            "store_state",
            "store_country",
            "revenue",
            "quantity_sold",
            "orders_count",
            "avg_check",
            "store_rank",
        )
    )

    supplier_window = Window.orderBy(F.col("revenue").desc())
    supplier_sales = (
        sales.groupBy(
            "supplier_id",
            "supplier_name",
            "supplier_contact",
            "supplier_city",
            "supplier_country",
        )
        .agg(
            F.round(F.sum("sale_total_price"), 2).alias("revenue"),
            F.sum("sale_quantity").cast("long").alias("quantity_sold"),
            F.countDistinct("sale_id").cast("long").alias("orders_count"),
            F.round(F.avg("product_price"), 2).alias("avg_product_price"),
        )
        .withColumn("supplier_rank", F.row_number().over(supplier_window))
        .select(
            "supplier_id",
            "supplier_name",
            "supplier_contact",
            "supplier_city",
            "supplier_country",
            "revenue",
            "quantity_sold",
            "orders_count",
            "avg_product_price",
            "supplier_rank",
        )
    )

    rating_sales_correlation = sales.stat.corr("product_rating", "sale_quantity")
    quality_window_high = Window.orderBy(F.col("product_rating").desc())
    quality_window_low = Window.orderBy(F.col("product_rating").asc())
    quality_window_reviews = Window.orderBy(F.col("product_reviews").desc())
    product_quality = (
        sales.groupBy(
            "product_id",
            "product_name",
            "product_category",
            "product_rating",
            "product_reviews",
        )
        .agg(
            F.sum("sale_quantity").cast("long").alias("quantity_sold"),
            F.round(F.sum("sale_total_price"), 2).alias("revenue"),
        )
        .withColumn("rating_rank_high", F.row_number().over(quality_window_high))
        .withColumn("rating_rank_low", F.row_number().over(quality_window_low))
        .withColumn("reviews_rank", F.row_number().over(quality_window_reviews))
        .withColumn("rating_sales_correlation", F.lit(rating_sales_correlation))
        .select(
            "product_id",
            "product_name",
            "product_category",
            "product_rating",
            "product_reviews",
            "quantity_sold",
            "revenue",
            "rating_rank_high",
            "rating_rank_low",
            "reviews_rank",
            "rating_sales_correlation",
        )
    )

    recreate_clickhouse_table(
        "report_product_sales",
        """
        product_id Int32,
        product_name Nullable(String),
        product_category Nullable(String),
        product_brand Nullable(String),
        quantity_sold Int64,
        revenue Float64,
        orders_count Int64,
        avg_order_amount Float64,
        avg_rating Float64,
        reviews_count Int32,
        sales_rank Int32
        """,
        "sales_rank, product_id",
    )
    recreate_clickhouse_table(
        "report_customer_sales",
        """
        customer_id Int32,
        customer_first_name Nullable(String),
        customer_last_name Nullable(String),
        customer_email Nullable(String),
        customer_country Nullable(String),
        customers_in_country Int64,
        total_spent Float64,
        orders_count Int64,
        avg_check Float64,
        customer_rank Int32
        """,
        "customer_rank, customer_id",
    )
    recreate_clickhouse_table(
        "report_time_sales",
        """
        sale_year Int32,
        sale_month Int32,
        revenue Float64,
        quantity_sold Int64,
        orders_count Int64,
        avg_order_amount Float64
        """,
        "sale_year, sale_month",
    )
    recreate_clickhouse_table(
        "report_store_sales",
        """
        store_id Int32,
        store_name Nullable(String),
        store_city Nullable(String),
        store_state Nullable(String),
        store_country Nullable(String),
        revenue Float64,
        quantity_sold Int64,
        orders_count Int64,
        avg_check Float64,
        store_rank Int32
        """,
        "store_rank, store_id",
    )
    recreate_clickhouse_table(
        "report_supplier_sales",
        """
        supplier_id Int32,
        supplier_name Nullable(String),
        supplier_contact Nullable(String),
        supplier_city Nullable(String),
        supplier_country Nullable(String),
        revenue Float64,
        quantity_sold Int64,
        orders_count Int64,
        avg_product_price Float64,
        supplier_rank Int32
        """,
        "supplier_rank, supplier_id",
    )
    recreate_clickhouse_table(
        "report_product_quality",
        """
        product_id Int32,
        product_name Nullable(String),
        product_category Nullable(String),
        product_rating Float64,
        product_reviews Int32,
        quantity_sold Int64,
        revenue Float64,
        rating_rank_high Int32,
        rating_rank_low Int32,
        reviews_rank Int32,
        rating_sales_correlation Float64
        """,
        "product_id",
    )

    write_clickhouse(product_sales, "report_product_sales")
    write_clickhouse(customer_sales, "report_customer_sales")
    write_clickhouse(time_sales, "report_time_sales")
    write_clickhouse(store_sales, "report_store_sales")
    write_clickhouse(supplier_sales, "report_supplier_sales")
    write_clickhouse(product_quality, "report_product_quality")

    print("All ClickHouse reports have been built.")
    spark.stop()


if __name__ == "__main__":
    main()
