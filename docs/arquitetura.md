# Arquitetura (Medalhão)

O fluxo segue o padrão **Landing → Bronze → Silver → Gold**.

## Camadas

| Camada | Conteúdo | Formato |
|--------|----------|---------|
| **Landing** | Extração bruta do Supabase (1 pasta/tabela em CSV) | CSV |
| **Bronze** | Espelho dos arquivos da landing, tipado como tabela Delta | Delta |
| **Silver** | Dados com regras de qualidade e integridade referencial leve | Delta |
| **Gold** | Modelo dimensional (Kimball) para análise | Delta |

## Convenção de schemas (exemplo)

Ajuste **catálogo** e **schema** conforme o padrão da disciplina (Unity Catalog é o mais comum).

- Landing de arquivos: `landing` / `dados` + **Volume** `raw` (recomendado) ou caminho `dbfs:/...`
- `bronze.bronze` — tabelas homônimas à origem (`clientes`, `produtos`, …)
- `silver.silver` — mesmos nomes, versão tratada
- `gold.gold` — `dim_*` e `fato_*`

## Modelo dimensional (Gold)

- `dim_cliente`, `dim_produto`, `dim_pedido`
- `fato_item_pedido` — granularidade **1 linha por item** do pedido

## Orquestração

Um **Databricks Job** com quatro tarefas em cadeia, cada uma dependente da anterior.

Veja [Job (orquestração)](job.md).
