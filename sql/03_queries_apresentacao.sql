-- Trabalho 3 — consultas para demonstração no Databricks (SQL)
-- Ajuste o catálogo se o seu for diferente de `workspace` (ex.: `main`).

-- =============================================================================
-- 1) Onde estão as tabelas (cada camada)
-- =============================================================================
SHOW TABLES IN workspace.bronze;
SHOW TABLES IN workspace.silver;
SHOW TABLES IN workspace.gold;

-- =============================================================================
-- 2) Bronze — amostra bruta (Delta vindo do CSV)
-- =============================================================================
SELECT 'bronze.clientes' AS tabela, COUNT(*) AS linhas FROM workspace.bronze.clientes;
SELECT * FROM workspace.bronze.clientes LIMIT 5;

SELECT 'bronze.produtos' AS tabela, COUNT(*) AS linhas FROM workspace.bronze.produtos;
SELECT * FROM workspace.bronze.produtos LIMIT 5;

SELECT 'bronze.pedidos' AS tabela, COUNT(*) AS linhas FROM workspace.bronze.pedidos;
SELECT * FROM workspace.bronze.pedidos LIMIT 5;

SELECT 'bronze.pedido_itens' AS tabela, COUNT(*) AS linhas FROM workspace.bronze.pedido_itens;
SELECT * FROM workspace.bronze.pedido_itens LIMIT 5;

-- =============================================================================
-- 3) Silver — dados com regras de qualidade
-- =============================================================================
SELECT 'silver.clientes' AS tabela, COUNT(*) AS linhas FROM workspace.silver.clientes;
SELECT * FROM workspace.silver.clientes LIMIT 5;

SELECT 'silver.pedido_itens' AS tabela, COUNT(*) AS linhas FROM workspace.silver.pedido_itens;
SELECT * FROM workspace.silver.pedido_itens LIMIT 5;

-- =============================================================================
-- 4) Gold — modelo Kimball (dimensões + fato)
-- =============================================================================
SELECT 'gold.dim_cliente' AS tabela, COUNT(*) AS linhas FROM workspace.gold.dim_cliente;
SELECT * FROM workspace.gold.dim_cliente ORDER BY sk_cliente LIMIT 10;

SELECT 'gold.dim_produto' AS tabela, COUNT(*) AS linhas FROM workspace.gold.dim_produto;
SELECT * FROM workspace.gold.dim_produto ORDER BY sk_produto LIMIT 10;

SELECT 'gold.dim_pedido' AS tabela, COUNT(*) AS linhas FROM workspace.gold.dim_pedido;
SELECT * FROM workspace.gold.dim_pedido ORDER BY sk_pedido LIMIT 10;

SELECT 'gold.fato_item_pedido' AS tabela, COUNT(*) AS linhas FROM workspace.gold.fato_item_pedido;
SELECT * FROM workspace.gold.fato_item_pedido LIMIT 15;

-- “Estrela” simples: fato + dimensões (para mostrar chaves)
SELECT
  f.pedido_item_id,
  f.dt_pedido,
  f.status,
  dc.nome AS cliente,
  dc.cidade,
  dp.sku,
  dp.nome AS produto,
  f.quantidade,
  f.preco_unitario,
  f.valor_item
FROM workspace.gold.fato_item_pedido AS f
LEFT JOIN workspace.gold.dim_cliente AS dc ON f.sk_cliente = dc.sk_cliente
LEFT JOIN workspace.gold.dim_produto AS dp ON f.sk_produto = dp.sk_produto
ORDER BY f.dt_pedido, f.pedido_item_id;
