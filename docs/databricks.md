# Databricks — notebooks

Os códigos-fonte estão em `notebooks/`:

| Arquivo | Etapa |
|---------|--------|
| `01_landing_extract.py` | Extração JDBC → CSV na landing |
| `02_bronze_ingest.py` | CSV → Delta em Bronze |
| `03_silver_quality.py` | Qualidade → Silver |
| `04_gold_kimball.py` | Kimball → Gold |

## Como usar no Workspace

1. Crie um notebook por etapa (ou importe o `.py` como notebook de Python).
2. No topo de cada notebook, ajuste:
   - `USE_UC_VOLUME`
   - Nomes de `CATALOG` / schemas
   - Caminho do Volume (se usar UC), criando o Volume previamente no catálogo correto.
3. Configure **Secrets** para `host`, `port`, `database`, `user`, `password`.

## Driver JDBC

O cluster precisa do driver PostgreSQL. Na maioria dos runtimes recentes do Databricks ele já está disponível. Se necessário, anexe a dependência Maven `org.postgresql:postgresql` compatível com o cluster.

## Landing com Unity Catalog Volume (recomendado)

Exemplo de caminho:

`/Volumes/landing/dados/raw/<tabela>/`

Crie o **Volume** `landing.dados.raw` (nomes exatos conforme sua conta) antes da primeira execução.
