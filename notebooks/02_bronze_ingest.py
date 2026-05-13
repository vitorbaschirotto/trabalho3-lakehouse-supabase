# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze: CSV (landing) → Delta

# COMMAND ----------

USE_UC_VOLUME = True

CATALOG = "landing"
LANDING_SCHEMA = "dados"

BRONZE_CATALOG = "bronze"
BRONZE_SCHEMA = "bronze"


def landing_base_path():
    if USE_UC_VOLUME:
        return f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/raw"
    return "dbfs:/mnt/landing/dados/raw"


base = landing_base_path()
files = dbutils.fs.ls(base)
table_dirs = [f.path for f in files if f.isDir()]

for d in table_dirs:
    table_name = d.rstrip("/").split("/")[-1]
    df = spark.read.option("header", True).option("mode", "PERMISSIVE").csv(d)

    full_table = f"`{BRONZE_CATALOG}`.`{BRONZE_SCHEMA}`.`{table_name}`"
    df.write.format("delta").mode("overwrite").saveAsTable(full_table)

# COMMAND ----------

# MAGIC %md
# MAGIC Bronze concluída. Próximo: `03_silver_quality`.
