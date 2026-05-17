import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "bigdata")
POSTGRES_USER = os.getenv("POSTGRES_USER", "bigdata")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "bigdata")

POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_PROPS = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}


def write_table(df, table_name):
    (
        df.write.mode("overwrite")
        .option("truncate", "false")
        .jdbc(POSTGRES_URL, table_name, properties=POSTGRES_PROPS)
    )
    print(f"Written PostgreSQL table: {table_name}, rows={df.count()}")


def add_surrogate_key(df, key_name, order_columns):
    window = Window.orderBy(*[F.col(c).asc_nulls_last() for c in order_columns])
    return df.withColumn(key_name, F.row_number().over(window))


def null_safe_join_condition(left_alias, right_alias, columns):
    condition = None
    for column in columns:
        part = F.col(f"{left_alias}.{column}").eqNullSafe(F.col(f"{right_alias}.{column}"))
        condition = part if condition is None else condition & part
    return condition


def main():
    spark = (
        SparkSession.builder.appName("BigDataSpark - star schema in PostgreSQL")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    source = spark.read.jdbc(
        POSTGRES_URL,
        "mock_data",
        properties=POSTGRES_PROPS,
    )

    customer = (
        source.select(
            F.col("sale_customer_id").alias("customer_id"),
            "customer_first_name",
            "customer_last_name",
            "customer_age",
            "customer_email",
            "customer_country",
            "customer_postal_code",
            "customer_pet_type",
            "customer_pet_name",
            "customer_pet_breed",
            "pet_category",
        )
        .dropDuplicates(["customer_id"])
    )

    seller = (
        source.select(
            F.col("sale_seller_id").alias("seller_id"),
            "seller_first_name",
            "seller_last_name",
            "seller_email",
            "seller_country",
            "seller_postal_code",
        )
        .dropDuplicates(["seller_id"])
    )

    product = (
        source.select(
            F.col("sale_product_id").alias("product_id"),
            "product_name",
            "product_category",
            "product_price",
            "product_quantity",
            "product_weight",
            "product_color",
            "product_size",
            "product_brand",
            "product_material",
            "product_description",
            "product_rating",
            "product_reviews",
            "product_release_date",
            "product_expiry_date",
        )
        .dropDuplicates(["product_id"])
    )

    store_natural_columns = [
        "store_name",
        "store_location",
        "store_city",
        "store_state",
        "store_country",
        "store_phone",
        "store_email",
    ]
    store = add_surrogate_key(
        source.select(*store_natural_columns).dropDuplicates(),
        "store_id",
        store_natural_columns,
    )

    supplier_natural_columns = [
        "supplier_name",
        "supplier_contact",
        "supplier_email",
        "supplier_phone",
        "supplier_address",
        "supplier_city",
        "supplier_country",
    ]
    supplier = add_surrogate_key(
        source.select(*supplier_natural_columns).dropDuplicates(),
        "supplier_id",
        supplier_natural_columns,
    )

    fact_columns = [
        "sale_id",
        "sale_date",
        "customer_id",
        "seller_id",
        "product_id",
        "store_id",
        "supplier_id",
        "sale_quantity",
        "sale_total_price",
    ]
    fact_window = Window.orderBy(
        F.col("source_id").asc_nulls_last(),
        F.col("customer_id").asc_nulls_last(),
        F.col("seller_id").asc_nulls_last(),
        F.col("product_id").asc_nulls_last(),
        F.col("sale_date").asc_nulls_last(),
        F.col("sale_total_price").asc_nulls_last(),
    )
    fact_sales = (
        source.alias("m")
        .join(
            store.alias("st"),
            null_safe_join_condition("m", "st", store_natural_columns),
            "left",
        )
        .join(
            supplier.alias("su"),
            null_safe_join_condition("m", "su", supplier_natural_columns),
            "left",
        )
        .select(
            F.col("m.id").alias("source_id"),
            F.col("m.sale_date"),
            F.col("m.sale_customer_id").alias("customer_id"),
            F.col("m.sale_seller_id").alias("seller_id"),
            F.col("m.sale_product_id").alias("product_id"),
            F.col("st.store_id"),
            F.col("su.supplier_id"),
            F.col("m.sale_quantity"),
            F.col("m.sale_total_price"),
        )
        .withColumn("sale_id", F.row_number().over(fact_window))
        .select(*fact_columns)
    )

    write_table(customer, "dim_customer")
    write_table(seller, "dim_seller")
    write_table(product, "dim_product")
    write_table(store, "dim_store")
    write_table(supplier, "dim_supplier")
    write_table(fact_sales, "fact_sales")

    print("Star schema has been built in PostgreSQL.")
    spark.stop()


if __name__ == "__main__":
    main()
