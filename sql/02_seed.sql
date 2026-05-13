-- Dados de exemplo para testar o pipeline (execute após 01_create_tables.sql)

insert into public.clientes (nome, email, cidade)
values
  ('Ana Souza', 'ana.souza@example.com', 'São Paulo'),
  ('Bruno Lima', 'bruno.lima@example.com', 'Curitiba'),
  ('Carla Dias', 'carla.dias@example.com', 'Belo Horizonte')
on conflict (email) do nothing;

insert into public.produtos (sku, nome, categoria, preco_lista)
values
  ('SKU-001', 'Camiseta Básica', 'Vestuário', 59.90),
  ('SKU-002', 'Caneca Térmica', 'Casa', 89.00),
  ('SKU-003', 'Fone Bluetooth', 'Eletrônicos', 199.90),
  ('SKU-004', 'Caderno A5', 'Papelaria', 24.50)
on conflict (sku) do nothing;

-- Pedidos + itens usando CTEs (Postgres)
with
c as (
  select cliente_id, email from public.clientes
  where email in ('ana.souza@example.com','bruno.lima@example.com')
),
p as (
  select produto_id, sku, preco_lista from public.produtos
  where sku in ('SKU-001','SKU-002','SKU-003','SKU-004')
),
ins_pedidos as (
  insert into public.pedidos (cliente_id, data_pedido, status)
  select c.cliente_id, timestamptz '2026-05-10 10:15:00-03', 'pago'
  from c where c.email = 'ana.souza@example.com'
  returning pedido_id, cliente_id
),
ins_pedidos2 as (
  insert into public.pedidos (cliente_id, data_pedido, status)
  select c.cliente_id, timestamptz '2026-05-11 16:40:00-03', 'enviado'
  from c where c.email = 'bruno.lima@example.com'
  returning pedido_id, cliente_id
)
insert into public.pedido_itens (pedido_id, produto_id, quantidade, preco_unitario)
select ip.pedido_id, pr.produto_id, v.qtd, v.pu
from ins_pedidos ip
cross join lateral (
  values
    ('SKU-001', 2, 59.90),
    ('SKU-004', 1, 24.50)
) as v(sku, qtd, pu)
join p pr on pr.sku = v.sku
union all
select ip2.pedido_id, pr.produto_id, v.qtd, v.pu
from ins_pedidos2 ip2
cross join lateral (
  values
    ('SKU-003', 1, 189.90),
    ('SKU-002', 2, 85.00)
) as v(sku, qtd, pu)
join p pr on pr.sku = v.sku;
