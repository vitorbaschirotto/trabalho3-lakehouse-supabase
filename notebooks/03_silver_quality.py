# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver: qualidade sobre Bronze → Delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

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

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

print(f"Catálogo usado: {UC_CATALOG}")


def read_bronze(name: str):
    return spark.table(f"`{UC_CATALOG}`.`{BRONZE_SCHEMA}`.`{name}`")


def write_silver(df, name: str):
    df.write.format("delta").mode("overwrite").saveAsTable(
        f"`{UC_CATALOG}`.`{SILVER_SCHEMA}`.`{name}`"
    )


# 1) CLIENTES
c0 = read_bronze("clientes")
c1 = (
    c0.withColumn("nome", F.trim(F.col("nome")))
    .withColumn("email", F.lower(F.trim(F.col("email"))))
    .withColumn("cidade", F.trim(F.col("cidade")))
    .where(F.col("nome") != "")
    .where(F.col("cidade") != "")
    .where(F.col("email").contains("@"))
)

w = Window.partitionBy("email").orderBy(F.col("data_cadastro").desc_nulls_last())
c2 = c1.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")
write_silver(c2, "clientes")

# 2) PRODUTOS
p0 = read_bronze("produtos")
p1 = (
    p0.withColumn("sku", F.upper(F.trim(F.col("sku"))))
    .withColumn("nome", F.trim(F.col("nome")))
    .withColumn("categoria", F.trim(F.col("categoria")))
    .withColumn("preco_lista", F.col("preco_lista").cast("decimal(12,2)"))
    .where(F.col("sku") != "")
    .where(F.col("nome") != "")
    .where(F.col("categoria") != "")
    .where(F.col("preco_lista") >= F.lit(0))
)

w2 = Window.partitionBy("sku").orderBy(F.col("nome"))
p2 = p1.withColumn("_rn", F.row_number().over(w2)).where(F.col("_rn") == 1).drop("_rn")
write_silver(p2, "produtos")

# 3) PEDIDOS
allowed = {"criado", "pago", "cancelado", "enviado", "entregue"}
pe0 = read_bronze("pedidos")
pe1 = (
    pe0.withColumn("status", F.trim(F.col("status")))
    .withColumn("data_pedido", F.col("data_pedido").cast("timestamp"))
    .where(F.col("status").isin(list(allowed)))
)

cli = spark.table(f"`{UC_CATALOG}`.`{SILVER_SCHEMA}`.`clientes`").select("cliente_id")
pe2 = pe1.join(cli, on="cliente_id", how="inner")
write_silver(pe2, "pedidos")

# 4) PEDIDO_ITENS
pi0 = read_bronze("pedido_itens")
pi1 = (
    pi0.withColumn("quantidade", F.col("quantidade").cast("int"))
    .withColumn("preco_unitario", F.col("preco_unitario").cast("decimal(12,2)"))
    .where(F.col("quantidade") > 0)
    .where(F.col("preco_unitario") >= F.lit(0))
)

ped = spark.table(f"`{UC_CATALOG}`.`{SILVER_SCHEMA}`.`pedidos`").select("pedido_id")
pr = spark.table(f"`{UC_CATALOG}`.`{SILVER_SCHEMA}`.`produtos`").select("produto_id")

pi2 = pi1.join(ped, on="pedido_id", how="inner").join(pr, on="produto_id", how="inner")
write_silver(pi2, "pedido_itens")

# COMMAND ----------

# MAGIC %md
# MAGIC Silver concluída. Próximo: `04_gold_kimball`.
