# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze: CSV (landing) → Delta
# MAGIC
# MAGIC Usa o **mesmo** catálogo e modo de landing que o notebook 01 (detecção abaixo).

# COMMAND ----------

LANDING_SCHEMA = "dados"
BRONZE_SCHEMA = "bronze"

_rows = spark.sql("SHOW CATALOGS").collect()
_catalog_list = [r[0] for r in _rows if len(r) > 0]

_priority = ("main", "hive_metastore", "workspace")
UC_CATALOG = next((c for c in _priority if c in _catalog_list), None)
if UC_CATALOG is None:
    _read_only = {"samples", "system"}
    _candidates = [c for c in _catalog_list if c not in _read_only]
    UC_CATALOG = _candidates[0] if _candidates else (_catalog_list[0] if _catalog_list else None)
if UC_CATALOG is None:
    raise RuntimeError("SHOW CATALOGS não retornou catálogos.")

USE_UC_VOLUME = UC_CATALOG in ("main", "workspace")
LANDING_DBFS_PATH = "dbfs:/tmp/trabalho3_landing_raw"

print(f"Catálogo usado: {UC_CATALOG} | USE_UC_VOLUME={USE_UC_VOLUME}")


def landing_base_path():
    if USE_UC_VOLUME:
        return f"/Volumes/{UC_CATALOG}/{LANDING_SCHEMA}/raw"
    return LANDING_DBFS_PATH


base = landing_base_path()
files = dbutils.fs.ls(base)
table_dirs = [f.path for f in files if f.isDir()]

for d in table_dirs:
    table_name = d.rstrip("/").split("/")[-1]
    df = spark.read.option("header", True).option("mode", "PERMISSIVE").csv(d)

    full_table = f"`{UC_CATALOG}`.`{BRONZE_SCHEMA}`.`{table_name}`"
    df.write.format("delta").mode("overwrite").saveAsTable(full_table)

# COMMAND ----------

# MAGIC %md
# MAGIC Bronze concluída. Próximo: `03_silver_quality`.
