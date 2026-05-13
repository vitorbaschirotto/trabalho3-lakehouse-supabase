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

## Conectividade com o Databricks

- Use connection string JDBC com `sslmode=require`.
- Guarde usuário e senha em **Databricks Secrets** (escopo sugerido: `supabase`).
- Se a conexão falhar, verifique restrições de rede / IP no painel do Supabase.

Credenciais **não** devem ir para o Git.
