# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Landing: extração Supabase (PostgreSQL) → CSV
# MAGIC
# MAGIC Pré-requisitos: Secrets no escopo `supabase` (`host`, `port`, `database`, `user`, `password`).

# COMMAND ----------

USE_UC_VOLUME = True

CATALOG = "landing"
LANDING_SCHEMA = "dados"
SOURCE_SCHEMA = "public"

BRONZE_CATALOG = "bronze"
BRONZE_SCHEMA = "bronze"

SILVER_CATALOG = "silver"
SILVER_SCHEMA = "silver"

GOLD_CATALOG = "gold"
GOLD_SCHEMA = "gold"


def landing_base_path():
    if USE_UC_VOLUME:
        return f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/raw"
    return "dbfs:/mnt/landing/dados/raw"


spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{LANDING_SCHEMA}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{BRONZE_CATALOG}`.`{BRONZE_SCHEMA}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{SILVER_CATALOG}`.`{SILVER_SCHEMA}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{GOLD_CATALOG}`.`{GOLD_SCHEMA}`")

# COMMAND ----------

host = dbutils.secrets.get(scope="supabase", key="host")
port = dbutils.secrets.get(scope="supabase", key="port")
database = dbutils.secrets.get(scope="supabase", key="database")
user = dbutils.secrets.get(scope="supabase", key="user")
password = dbutils.secrets.get(scope="supabase", key="password")

jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}?sslmode=require"

props = {
    "user": user,
    "password": password,
    "driver": "org.postgresql.Driver",
}

tables_df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("driver", props["driver"])
    .option("user", props["user"])
    .option("password", props["password"])
    .option(
        "dbtable",
        f"(SELECT table_schema, table_name "
        f"FROM information_schema.tables "
        f"WHERE table_schema = '{SOURCE_SCHEMA}' AND table_type = 'BASE TABLE') AS t",
    )
    .load()
)

base = landing_base_path()
for row in tables_df.collect():
    schema_name = row["table_schema"]
    table_name = row["table_name"]
    fq = f"{schema_name}.{table_name}"

    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("driver", props["driver"])
        .option("user", props["user"])
        .option("password", props["password"])
        .option("dbtable", fq)
        .load()
    )

    out_dir = f"{base}/{table_name}/"
    dbutils.fs.rm(out_dir, recurse=True)

    df.coalesce(1).write.mode("overwrite").option("header", True).csv(out_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC Landing concluída. Próximo passo: notebook `02_bronze_ingest`.
