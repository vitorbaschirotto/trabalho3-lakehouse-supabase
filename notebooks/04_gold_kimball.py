# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Gold: modelagem Kimball (dims + fato)

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

SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

print(f"Catálogo usado: {UC_CATALOG}")

clientes = spark.table(f"`{UC_CATALOG}`.`{SILVER_SCHEMA}`.`clientes`")
produtos = spark.table(f"`{UC_CATALOG}`.`{SILVER_SCHEMA}`.`produtos`")
pedidos = spark.table(f"`{UC_CATALOG}`.`{SILVER_SCHEMA}`.`pedidos`")
itens = spark.table(f"`{UC_CATALOG}`.`{SILVER_SCHEMA}`.`pedido_itens`")


def sk_from_id(df, id_col, sk_name):
    w = Window.orderBy(F.col(id_col).cast("string"))
    return df.withColumn(sk_name, F.row_number().over(w))


dim_cliente = sk_from_id(
    clientes.select(
        "cliente_id",
        "nome",
        "email",
        "cidade",
        F.to_date("data_cadastro").alias("dt_cadastro"),
    ),
    "cliente_id",
    "sk_cliente",
)

dim_produto = sk_from_id(
    produtos.select("produto_id", "sku", "nome", "categoria", "preco_lista"),
    "produto_id",
    "sk_produto",
)

pedidos_com_sk_cliente = pedidos.join(
    dim_cliente.select("cliente_id", "sk_cliente"),
    on="cliente_id",
    how="left",
)

dim_pedido = sk_from_id(
    pedidos_com_sk_cliente.select(
        "pedido_id",
        "sk_cliente",
        "data_pedido",
        "status",
    ),
    "pedido_id",
    "sk_pedido",
)

fato_item_pedido = (
    itens.join(
        dim_pedido.select("pedido_id", "sk_pedido", "sk_cliente", "data_pedido", "status"),
        on="pedido_id",
        how="left",
    )
    .join(dim_produto.select("produto_id", "sk_produto"), on="produto_id", how="left")
    .withColumn("valor_item", F.col("quantidade") * F.col("preco_unitario"))
    .select(
        "pedido_item_id",
        "sk_pedido",
        "sk_cliente",
        "sk_produto",
        F.to_date("data_pedido").alias("dt_pedido"),
        "status",
        F.col("quantidade").cast("int").alias("quantidade"),
        F.col("preco_unitario").cast("decimal(12,2)").alias("preco_unitario"),
        F.col("valor_item").cast("decimal(14,2)").alias("valor_item"),
    )
)

dim_cliente.write.format("delta").mode("overwrite").saveAsTable(
    f"`{UC_CATALOG}`.`{GOLD_SCHEMA}`.`dim_cliente`"
)
dim_produto.write.format("delta").mode("overwrite").saveAsTable(
    f"`{UC_CATALOG}`.`{GOLD_SCHEMA}`.`dim_produto`"
)
dim_pedido.write.format("delta").mode("overwrite").saveAsTable(
    f"`{UC_CATALOG}`.`{GOLD_SCHEMA}`.`dim_pedido`"
)
fato_item_pedido.write.format("delta").mode("overwrite").saveAsTable(
    f"`{UC_CATALOG}`.`{GOLD_SCHEMA}`.`fato_item_pedido`"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Gold concluída. Tabelas: `dim_cliente`, `dim_produto`, `dim_pedido`, `fato_item_pedido`.
