# Atividade 03 — Notebook Gold (Camada Gold)

Este README documenta o que foi implementado no notebook `Atividade3_GoldLayer.ipynb` (camada Gold) do pipeline Medalhão do e‑commerce.

## Objetivo

Construir a camada Gold com agregações, fatos e views analíticas prontos para consumo por dashboards e análise estratégica. As transformações combinam informações das camadas Bronze e Silver para gerar visões de vendas por localidade, performance logística, sazonalidade e ranking de produtos/vendedores.

## Principais projetos implementados

1. Projeto 1 — Vendas por Localidade
   - Objetivo: identificar cidades/estados com maior volume de vendas para decisões logísticas (CDs, rotas).
   - Fato criado: `gold.ft_vendas_consumidor_local` — granularidade por pedido com `id_pedido`, `id_consumidor`, `valor_total_pedido_brl`, `cidade`, `estado`, `data_pedido`.
   - View analítica: `gold.view_total_compras_por_consumidor` — agregação por `cidade` e `estado` com `quantidade_vendas` e `valor_total_localidade`.
   - Consulta exemplo: total de vendas por estado (ordenado por receita).

2. Projeto 2 — Análise de Atrasos
   - Objetivo: monitorar SLA de entregas, identificar regiões críticas e avaliar a pontualidade dos vendedores.
   - Fato criado: `gold.ft_atrasos_pedidos_local_vendedor` — cruzamento entre vendedor, cliente e prazos (`entrega_no_prazo`, `tempo_entrega_dias`, `tempo_entrega_estimado_dias`, `cidade`, `estado`).
   - Views criadas:
     - `gold.view_tempo_medio_entrega_localidade` — compara tempo médio real vs estimado por localidade.
     - `gold.view_vendedor_pontualidade` — ranking de vendedores por percentual de atraso.

3. Projeto 3 — Análise de Performance (Vendas Gerais)
   - Objetivos: visão 360º das vendas com granularidade temporal e categórica (produtos, tickets, avaliação).
   - Dimensão: `gold.dm_tempo` — calendário gerado entre a menor e maior data dos pedidos (`sk_tempo`, `ano`, `trimestre`, `mes`, `dia`, dia da semana, flags de fim de semana etc.).
   - Fato geral: `gold.ft_vendas_geral` — unificação de itens, pedidos, cotação e avaliações com métricas em BRL e USD (`valor_produto_brl`, `valor_frete_brl`, `valor_total_item_brl`, `valor_total_item_usd`, `cotacao_dolar`, `avaliacao_pedido`).
   - Views analíticas:
     - `gold.view_vendas_por_periodo` — agregações por `ano`, `trimestre`, `mes`, `dia` com receita, ticket médio e avaliação média.
     - `gold.view_top_produto` — ranking de produtos por receita, quantidade vendida, ticket e avaliação.
     - `gold.view_vendas_produtos_esteticos` — visão especializada filtrando categoria (ex.: `fashion%`).

4. Projeto 4 — Orquestração (descrição)
   - Estratégia: agendamento cron (ex.: a cada 2 horas) com dependência linear (Bronze → Silver → Gold).
   - Exemplo: seção contém um trecho YAML de job que executa notebooks em sequência e evita sobreposição de execuções.

## Tabelas / Views Gold geradas

- Tabelas Gold:
  - `workspace.gold.ft_vendas_consumidor_local`
  - `workspace.gold.ft_atrasos_pedidos_local_vendedor`
  - `workspace.gold.dm_tempo`
  - `workspace.gold.ft_vendas_geral`

- Views analíticas (exemplos):
  - `workspace.gold.view_total_compras_por_consumidor`
  - `workspace.gold.view_tempo_medio_entrega_localidade`
  - `workspace.gold.view_vendedor_pontualidade`
  - `workspace.gold.view_vendas_por_periodo`
  - `workspace.gold.view_top_produto`
  - `workspace.gold.view_vendas_produtos_esteticos`

## Dependências e pré‑requisitos

- Ambiente Spark 3.x com suporte a Delta Lake (escrita `format("delta")`).
- Camada Silver (`workspace.silver.*`) previamente populada com as tabelas:
  - `ft_consumidores`, `ft_pedido_total`, `ft_itens_pedidos`, `ft_pedidos`, `dm_cotacao_dolar`, `ft_avaliacoes_pedidos`, `ft_produtos`.
- Acesso ao cluster/notebook runner (Databricks/Local Spark) e permissões para criar tabelas/views no catálogo `workspace.gold`.

## Notas de implementação e decisões importantes

- Escrita em Delta com `mode("overwrite")` em várias etapas — apropriado para desenvolvimento, mas em produção recomenda-se uso de `append` + deduplicação, versionamento Delta (Time Travel) e políticas de retenção.
- As joins principais usam chaves estrangeiras geradas nas camadas anteriores (ex.: `id_pedido`, `id_consumidor`, `id_vendedor`, `id_produto`); integridade referencial deve ser verificada na Silver.
- Conversão BRL → USD é feita usando a cotação por data (`dm_cotacao_dolar`) — garante consistência histórica dos valores.
- Views são criadas com `CREATE OR REPLACE VIEW` e SQL diretamente no notebook (%sql), permitindo consumo por ferramentas de BI.
