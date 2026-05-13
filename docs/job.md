# Job — encadeamento sequencial

## Objetivo

Garantir que **Landing → Bronze → Silver → Gold** rode em ordem, sem paralelismo entre etapas.

## Passos no Databricks

1. Abra **Workflows** / **Jobs** e crie um novo Job.
2. Adicione **Task** 1: notebook `01_landing_extract`.
3. Adicione **Task** 2: notebook `02_bronze_ingest` — marque dependência da Task 1.
4. Adicione **Task** 3: notebook `03_silver_quality` — dependência da Task 2.
5. Adicione **Task** 4: notebook `04_gold_kimball` — dependência da Task 3.
6. Escolha um **cluster** ou **policy** compatível com a turma (Free Edition pode ter limites de tempo).

## Boas práticas

- Use o mesmo cluster (Job cluster) para todas as tasks, se permitido, reduzindo tempo de provisionamento.
- Armazene parâmetros sensíveis em Secrets; o Job injeta no runtime dos notebooks.

Se a disciplina exigir explicitamente **Delta Live Tables (DLT)**, substitua a Bronze–Silver–Gold por pipelines DLT — o encadeamento continua sendo um Job que dispara o pipeline ou tasks DLT.
