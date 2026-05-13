# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Landing: extração Supabase (PostgreSQL) → CSV
# MAGIC
# MAGIC ### Host para o Databricks (obrigatório ler)
# MAGIC
# MAGIC O host **`db.omqjqczvdstcqhyvrwyd.supabase.co`** é conexão **direta** → no Databricks costuma ser **só IPv6** → erro *Network unreachable*. **Não use esse host** em `SUPABASE_SESSION_POOLER_HOST`.
# MAGIC
# MAGIC **Passos no Supabase**
# MAGIC
# MAGIC 1. Abra o projeto → botão **Connect** (canto superior direito).  
# MAGIC 2. Em **Método / type / pooler**, selecione **Session pooler** (às vezes “Session mode”).  
# MAGIC 3. Copie a **URI** ou o **User** exatos dessa opção. O user costuma ser **`postgres.ALGO`** — esse **ALGO pode ser diferente** do trecho em `db....supabase.co` (vide doc Supabase).  
# MAGIC 4. No notebook: preferindo colar a URI inteira em **`SUPABASE_SESSION_POOLER_URI`** (senha dentro da URI → não commite no Git).  
# MAGIC Doc: [Connect to Postgres](https://supabase.com/docs/guides/database/connecting-to-postgres). Opcional: Secrets Databricks escopo `supabase`. Catálogo `samples` é só leitura; o notebook prefere `workspace` / `main`.

# COMMAND ----------

LANDING_SCHEMA = "dados"
SOURCE_SCHEMA = "public"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

_rows = spark.sql("SHOW CATALOGS").collect()
_catalog_list = [r[0] if len(r) > 0 else None for r in _rows]
_catalog_list = [c for c in _catalog_list if c]

# Ordem: main / hive_metastore / workspace (onde alunos costumam criar schema).
# NÃO usar "samples" como padrão — em muitos workspaces é só leitura (CREATE SCHEMA negado).
_priority = ("main", "hive_metastore", "workspace")
UC_CATALOG = next((c for c in _priority if c in _catalog_list), None)
if UC_CATALOG is None:
    _read_only = {"samples", "system"}
    _candidates = [c for c in _catalog_list if c not in _read_only]
    UC_CATALOG = _candidates[0] if _candidates else (_catalog_list[0] if _catalog_list else None)
if UC_CATALOG is None:
    raise RuntimeError("SHOW CATALOGS não retornou catálogos.")

# Landing em Volume UC (recomendado): DBFS público (/FileStore, /tmp) costuma estar desativado [DBFS_DISABLED].
USE_UC_VOLUME = UC_CATALOG in ("main", "workspace")
LANDING_DBFS_PATH = "dbfs:/tmp/trabalho3_landing_raw"  # só se USE_UC_VOLUME False e DBFS permitido

print(f"Catálogo usado: {UC_CATALOG} | USE_UC_VOLUME={USE_UC_VOLUME}")
print("Catálogos disponíveis:", _catalog_list)


def landing_base_path():
    if USE_UC_VOLUME:
        return f"/Volumes/{UC_CATALOG}/{LANDING_SCHEMA}/raw"
    return LANDING_DBFS_PATH


spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{UC_CATALOG}`.`{LANDING_SCHEMA}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{UC_CATALOG}`.`{BRONZE_SCHEMA}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{UC_CATALOG}`.`{SILVER_SCHEMA}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{UC_CATALOG}`.`{GOLD_SCHEMA}`")

if USE_UC_VOLUME:
    try:
        spark.sql(
            f"CREATE VOLUME IF NOT EXISTS `{UC_CATALOG}`.`{LANDING_SCHEMA}`.`raw`"
        )
    except Exception as e:
        print(f"ERRO ao criar Volume `{UC_CATALOG}`.`{LANDING_SCHEMA}`.`raw`: {e}")
        print("Peça permissão CREATE VOLUME no catálogo ou use storage indicado pela faculdade.")

# COMMAND ----------

# --- Conexão Supabase (PostgreSQL) ---
# Doc: https://supabase.com/docs/guides/database/connecting-to-postgres
#
# Schema das tabelas no Postgres (Supabase); se rodar só esta célula, o default é "public".
SOURCE_SCHEMA = globals().get("SOURCE_SCHEMA", "public")
# Reference: o texto em db.<algo>.supabase.co pode ser DIFERENTE do sufixo em postgres.<algo>
# no Session pooler (doc Supabase). Por isso o mais seguro é colar a URI ou o user do Connect.
PROJECT_REF = "omqjqczvdstcqhyvrwyd"
#
# Escolha UMA opção para o Databricks (IPv4):
# - "session"     → Session pooler (IPv4): use SUPABASE_SESSION_POOLER_URI (recomendado) ou host+user do Connect.
# - "transaction" → db.{REF}.supabase.co:6543 — no Databricks costuma ser só IPv6 → falha (Errno 101).
# - "direct"      → db....:5432 — idem.
SUPABASE_CONNECT_MODE = "session"  # use "session" no Databricks

# Session pooler (IPv4 no Databricks).
# Host típico: aws-<0 ou 1>-<REGIÃO>.pooler.supabase.com
# Se TCP OK mas **FATAL: Tenant or user not found**: muito comum o host correto ser **aws-1-** (não aws-0-).
# Confirme em Supabase > **Connect > Session pooler** (copie Host) ou use SHARD = "1" primeiro.
# Região do pooler = região do projeto no painel Connect (ex.: URI ...aws-1-us-east-1.pooler... → us-east-1).
SUPABASE_AWS_REGION = "us-east-1"  # era sa-east-1 no exemplo; ajuste ao que aparecer na SUA URI
SUPABASE_POOLER_SHARD = "1"        # alinhar à URI (aws-0- vs aws-1-)
SUPABASE_SESSION_POOLER_HOST_OVERRIDE = ""  # melhor: cole o host completo do painel (ex.: aws-1-sa-east-1.pooler.supabase.com)
SUPABASE_SESSION_POOLER_HOST = (
    SUPABASE_SESSION_POOLER_HOST_OVERRIDE.strip()
    if (SUPABASE_SESSION_POOLER_HOST_OVERRIDE or "").strip()
    else f"aws-{SUPABASE_POOLER_SHARD}-{SUPABASE_AWS_REGION}.pooler.supabase.com"
)

# Opcional mas RECOMENDADO: cole a URI de Connect → Session pooler entre aspas (substitua [YOUR-PASSWORD]).
# Ex.: postgresql://postgres.omqjqczvdstcqhyvrwyd:SENHA@aws-1-us-east-1.pooler.supabase.com:5432/postgres
# Se senha tiver @ ou :, prefira URI sem senha e use SUPABASE_PASSWORD abaixo, ou encode na URI.
SUPABASE_SESSION_POOLER_URI = ""

# Sem URI: ajuste host (OVERRIDE/SHARD) e user (pode ser diferente de postgres.PROJECT_REF → copie do Connect).
SUPABASE_POOLER_USER_OVERRIDE = ""  # ex.: "postgres.abcxyz" exatamente como no painel Session pooler


def _host_from_connection_string(value: str) -> str:
    """Host nu ('aws-...pooler...') ou URI postgres://... (senha com @ pode quebrar split manual)."""
    from urllib.parse import urlparse

    v = (value or "").strip().strip('"').strip("'")
    if not v:
        return ""
    if "://" in v:
        try:
            return (urlparse(v).hostname or "").strip()
        except Exception:
            return v
    return v


def _parse_postgres_uri(uri: str):
    """Retorna (host, port, user, password, database) ou None se inválido."""
    from urllib.parse import urlparse, unquote

    u = (uri or "").strip().strip('"').strip("'")
    if not u:
        return None
    p = urlparse(u)
    host = (p.hostname or "").strip()
    if not host:
        return None
    port = str(p.port or 5432)
    user = unquote(p.username) if p.username else ""
    password = unquote(p.password) if p.password else ""
    database = ((p.path or "/postgres").strip("/") or "postgres").split("?")[0]
    return host, port, user, password, database


USE_DATABRICKS_SECRETS = False
# NUNCA commite senha no GitHub.
SUPABASE_PASSWORD = "COLOQUE_SUA_SENHA_AQUI"

if USE_DATABRICKS_SECRETS:
    host = dbutils.secrets.get(scope="supabase", key="host")
    port = dbutils.secrets.get(scope="supabase", key="port")
    database = dbutils.secrets.get(scope="supabase", key="database")
    user = dbutils.secrets.get(scope="supabase", key="user")
    password = dbutils.secrets.get(scope="supabase", key="password")
elif SUPABASE_CONNECT_MODE == "session":
    _uri = (SUPABASE_SESSION_POOLER_URI or "").strip()
    _parsed = _parse_postgres_uri(_uri) if _uri else None
    if _parsed:
        host, port, user, _pw_uri, database = _parsed
        password = _pw_uri if _pw_uri else SUPABASE_PASSWORD
        if not user:
            raise ValueError(
                "URI do Session pooler sem usuário. Cole a URI completa do Connect (user tipo postgres.xxxxx)."
            )
    else:
        _ov = (SUPABASE_SESSION_POOLER_HOST_OVERRIDE or "").strip()
        _raw_pooler = _ov if _ov else f"aws-{SUPABASE_POOLER_SHARD}-{SUPABASE_AWS_REGION}.pooler.supabase.com"
        _h = _host_from_connection_string(_raw_pooler)
        _bad_db = _h.startswith("db.") and ".supabase.co" in _h
        if not _h:
            raise ValueError(
                "Host do pooler vazio. Cole SUPABASE_SESSION_POOLER_URI (Connect → Session pooler) "
                "ou defina região/shard + SUPABASE_SESSION_POOLER_HOST_OVERRIDE."
            )
        if _bad_db:
            raise ValueError(
                "Host 'db....supabase.co' não é pooler. Use URI de Session pooler ou host aws-*-....pooler...."
            )
        host = _h
        port = "5432"
        database = "postgres"
        _u = (SUPABASE_POOLER_USER_OVERRIDE or "").strip()
        user = _u if _u else f"postgres.{PROJECT_REF}"
        password = SUPABASE_PASSWORD
        print(
            "Aviso: usando user montado ou OVERRIDE. Se der tenant/user not found, "
            "preencha SUPABASE_SESSION_POOLER_URI ou SUPABASE_POOLER_USER_OVERRIDE com o valor do Connect "
            "(o sufixo após postgres. pode ser ≠ ao texto em db....supabase.co)."
        )
elif SUPABASE_CONNECT_MODE == "transaction":
    host = f"db.{PROJECT_REF}.supabase.co"
    port = "6543"
    database = "postgres"
    user = "postgres"
    password = SUPABASE_PASSWORD
else:
    host = f"db.{PROJECT_REF}.supabase.co"
    port = "5432"
    database = "postgres"
    user = "postgres"
    password = SUPABASE_PASSWORD

# Transaction pooler exige desligar prepared statements no driver JDBC (Spark usa prepared).
_jdbc_extra = "&prepareThreshold=0" if SUPABASE_CONNECT_MODE == "transaction" else ""
jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}?sslmode=require{_jdbc_extra}"

print(
    f"JDBC → host={host} port={port} db={database} user={user} mode={SUPABASE_CONNECT_MODE}"
)

# Diagnóstico TCP: tenta TODOS os endereços (IPv4 e IPv6). Erro [Errno 101] em IPv6 comum no
# host db.*.supabase.co em compute só-IPv4 → use modo "session" + host pooler do painel Connect.
RUN_TCP_TEST = True
if RUN_TCP_TEST:
    import socket

    try:
        infos = socket.getaddrinfo(host, int(port), type=socket.SOCK_STREAM)
    except socket.gaierror as e:
        print(f"Teste TCP: DNS falhou para {host} → {e}")
        infos = []

    ok = False
    for _fam, _type, _proto, _canon, sockaddr in infos:
        label = "IPv6" if _fam == socket.AF_INET6 else "IPv4"
        try:
            s = socket.socket(_fam, _type)
            s.settimeout(15)
            s.connect(sockaddr)
            s.close()
            print(f"Teste TCP: OK via {label} → {sockaddr}")
            ok = True
            break
        except OSError as e:
            print(f"Teste TCP: falhou {label} {sockaddr} → {e}")

    if not ok and infos:
        print(
            "Nenhum endereço alcançável. Se só IPv6 falhou com 101: use Session pooler "
            "(host aws-0-....pooler.supabase.com) ou peça cluster com IPv4 para o host db.*."
        )
    if not infos:
        print(
            "Ações: (1) Supabase ativo. (2) Cluster All-Purpose (não serverless). "
            f"(3) Connect > Session pooler — host aws-0-....pooler.supabase.com, porta 5432, "
            f"user postgres.{PROJECT_REF}"
        )

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

if "landing_base_path" not in globals():
    raise RuntimeError(
        "Rode antes a 1ª célula de código (SHOW CATALOGS + CREATE SCHEMA + def landing_base_path), "
        "ou use **Run all** de cima para baixo."
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
