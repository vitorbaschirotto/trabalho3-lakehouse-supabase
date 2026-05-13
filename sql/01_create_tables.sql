-- Trabalho 3 — esquema exemplo (loja) no Supabase / PostgreSQL
-- Execute no SQL Editor do Supabase.

create extension if not exists "pgcrypto";

create table if not exists public.clientes (
  cliente_id     uuid primary key default gen_random_uuid(),
  nome           text not null,
  email          text not null unique,
  cidade         text not null,
  data_cadastro  timestamptz not null default now()
);

create table if not exists public.produtos (
  produto_id     uuid primary key default gen_random_uuid(),
  sku            text not null unique,
  nome           text not null,
  categoria      text not null,
  preco_lista    numeric(12,2) not null check (preco_lista >= 0)
);

create table if not exists public.pedidos (
  pedido_id      uuid primary key default gen_random_uuid(),
  cliente_id     uuid not null references public.clientes(cliente_id),
  data_pedido    timestamptz not null default now(),
  status         text not null check (status in ('criado','pago','cancelado','enviado','entregue'))
);

create table if not exists public.pedido_itens (
  pedido_item_id uuid primary key default gen_random_uuid(),
  pedido_id      uuid not null references public.pedidos(pedido_id) on delete cascade,
  produto_id     uuid not null references public.produtos(produto_id),
  quantidade     int not null check (quantidade > 0),
  preco_unitario numeric(12,2) not null check (preco_unitario >= 0)
);

create index if not exists idx_pedidos_cliente on public.pedidos(cliente_id);
create index if not exists idx_pedido_itens_pedido on public.pedido_itens(pedido_id);
create index if not exists idx_pedido_itens_produto on public.pedido_itens(produto_id);
