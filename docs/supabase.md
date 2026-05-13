# Supabase (PostgreSQL)

## Pré-requisitos

- Projeto Supabase ativo
- Acesso ao **SQL Editor**

## Passos

1. Abra o arquivo `sql/01_create_tables.sql` e execute no Supabase.
2. Execute `sql/02_seed.sql` para inserir dados de exemplo (pedidos e itens).

## Tabelas

- `public.clientes`
- `public.produtos`
- `public.pedidos`
- `public.pedido_itens`

## Databricks e IPv4 (Session pooler)

O host **direto** `db.<ref>.supabase.co` costuma resolver **só IPv6**. Muitos clusters Databricks são **IPv4** → erro `Network is unreachable` / JDBC *connection attempt failed*.

**Solução:** no Supabase use **Connect** (topo do projeto) → **Session pooler** → copie o **Host** (formato típico `aws-0-<região>.pooler.supabase.com`, **não** começa com `db.`). No notebook `01`, preencha `SUPABASE_SESSION_POOLER_HOST` com esse host (ou cole a URI completa dessa opção; se tiver senha na URI, **não** commite no Git).

- Porta: **5432**
- Usuário JDBC no modo session: **`postgres.<PROJECT_REF>`** (o `PROJECT_REF` é o mesmo texto que aparece em `db.<PROJECT_REF>.supabase.co`)

Referência: [Connect to your database](https://supabase.com/docs/guides/database/connecting-to-postgres).

## Conectividade geral

- JDBC com `sslmode=require`.
- Credenciais preferencialmente em **Databricks Secrets** (escopo sugerido: `supabase`).
- Restrições de IP / rede no painel do Supabase podem bloquear o cluster.

Credenciais **não** devem ir para o Git.
