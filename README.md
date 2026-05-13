# Trabalho 3 — Lakehouse no Databricks (Arquitetura Medalhão)

Projeto acadêmico: extração do **Supabase (PostgreSQL)** para camadas **LANDING → BRONZE → SILVER → GOLD**, com **Delta Lake** e orquestração via **Databricks Job** (execução sequencial).

## Stack

- **Origem:** Supabase (PostgreSQL)
- **Formato na landing:** CSV (dados relacionais)
- **Camadas internas:** Delta Lake (Bronze, Silver, Gold)
- **Modelagem analítica:** Ralph Kimball (dimensões + fato)

## Estrutura do repositório

| Pasta | Conteúdo |
|--------|----------|
| `sql/` | Script de criação de tabelas e carga de exemplo no Supabase |
| `notebooks/` | Código dos notebooks (PySpark), um arquivo por etapa |
| `docs/` | Documentação servida pelo **MkDocs** |

## Documentação (MkDocs no GitHub Pages)

- **Site publicado:** [https://vitorbaschirotto.github.io/trabalho3-lakehouse-supabase/](https://vitorbaschirotto.github.io/trabalho3-lakehouse-supabase/)
- **Código-fonte da doc:** pasta `docs/` e `mkdocs.yml` (branch `main`); build deployado na branch `gh-pages` com `mkdocs gh-deploy`.

Se o link abrir **404**, no repositório GitHub vá em **Settings → Pages** e configure **Build and deployment** com fonte **Deploy from a branch**, branch **`gh-pages`**, pasta **`/` (root)**. Aguarde alguns minutos e atualize a página.

Opcional: na página principal do repositório, em **About** (ícone de engrenagem), adicione o mesmo URL em **Website**.

## Início rápido

1. Execute os scripts em `sql/` no SQL Editor do Supabase.
2. No Databricks, crie **Secrets** (ex.: escopo `supabase`) com host, port, database, user, password.
3. Ajuste catálogos/schemas no início de cada notebook em `notebooks/` conforme o ambiente da turma (Unity Catalog ou metastore).
4. Importe ou cole cada arquivo `.py` em um notebook no Workspace.
5. Crie um **Job** encadeando: `01_landing` → `02_bronze` → `03_silver` → `04_gold`.

Detalhes no [site MkDocs](https://vitorbaschirotto.github.io/trabalho3-lakehouse-supabase/) ou no [fonte em Markdown](docs/index.md). Localmente, após instalar dependências:

```bash
pip install mkdocs-material
mkdocs serve
```

## Job (sequência)

1. **01_landing_extract** — JDBC no Postgres; grava CSV na landing (`Volume` ou `DBFS`).
2. **02_bronze_ingest** — lê CSV; grava Delta em `bronze`.
3. **03_silver_quality** — regras de qualidade; grava Delta em `silver`.
4. **04_gold_kimball** — dimensões e fato; grava Delta em `gold`.

## Avisos

- Não commite credenciais. Use **Databricks Secrets**.
- Confirme conectividade Supabase ↔ Databricks (SSL, firewall, allowlist de IP se aplicável).

## Publicar no GitHub

Na pasta do projeto:

```bash
git init
git add .
git commit -m "Trabalho 3: pipeline Medalhão com Supabase e Databricks"
git branch -M main
git remote add origin https://github.com/SEU_USUARIO/trabalho3-lakehouse-supabase.git
git push -u origin main
```

Substitua `SEU_USUARIO` e o nome do repositório. Crie o repositório **vazio** no GitHub antes do `push` (sem README, se quiser evitar conflito na primeira subida).

Com [GitHub CLI](https://cli.github.com/) instalado e autenticado:

```bash
gh repo create trabalho3-lakehouse-supabase --private --source=. --remote=origin --push
```

## Licença

Uso educacional — ajuste conforme solicitado pela disciplina.
