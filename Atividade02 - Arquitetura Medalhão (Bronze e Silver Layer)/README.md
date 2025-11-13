
# Atividade 02 — Arquitetura Medalhão (Bronze & Silver Layer)

Este documento sintetiza o trabalho feito nos notebooks desta pasta:

- `notebooks/Atividade2_BronzeLayer.ipynb` — ingestão e persistência inicial (camada Bronze);
- `notebooks/Atividade2_SilverLayer.ipynb` — tratamento, normalização e criação de tabelas da camada Silver.

Baseei o formato deste README no padrão usado em `Atividade01 - Pyspark/README.md` e resumi as etapas, tabelas e decisões tomadas durante a atividade.

**Datasets usados (pasta `dados/`):**

- `olist_customers_dataset.csv` — consumidores
- `olist_geolocation_dataset.csv` — geolocalizações
- `olist_order_items_dataset.csv` — itens de pedido
- `olist_order_payments_dataset.csv` — pagamentos
- `olist_order_reviews_dataset.csv` — avaliações
- `olist_orders_dataset.csv` — pedidos
- `olist_products_dataset.csv` — produtos
- `olist_sellers_dataset.csv` — vendedores
- `product_category_name_translation.csv` — tradução de categorias (mapping PT/EN)

---

## Objetivo geral

Construir um pipeline em arquitetura medalhão para o dataset do e-commerce brasileiro:

- Bronze: ingestão raw dos CSVs (com `ingestion_timestamp` para rastreabilidade) e carregamento em tabelas Delta;
- Silver: limpeza, normalização, deduplicação, tradução de rótulos, cálculo de métricas e criação de tabelas prontas para consumo analítico.

---

## O que foi feito — resumo por notebook

1) `Atividade2_BronzeLayer.ipynb` (Bronze)

- Leitura dos CSVs listados acima a partir do volume local (path paramétrico). Cada arquivo foi lido com header e schema inferido.
- Adição da coluna `ingestion_timestamp` com `current_timestamp()` para manter histórico de ingestão.
- Persistência em formato Delta nas tabelas do schema Bronze (`workspace.bronze.*`), sobrescrevendo (modo `overwrite`) com `overwriteSchema=true`.
- Extração adicional: consulta à API do Banco Central para recuperar as cotações do dólar por período (ano a ano), conversão em DataFrame e persistência na tabela `bronze.dm_cotacao_dolar`.

Tabelas Bronze geradas (nomes usados no notebook):

- `workspace.bronze.ft_consumidores`
- `workspace.bronze.ft_geolocalizacao`
- `workspace.bronze.ft_itens_pedidos`
- `workspace.bronze.ft_pagamentos_pedidos`
- `workspace.bronze.ft_avaliacoes_pedidos`
- `workspace.bronze.ft_pedidos`
- `workspace.bronze.ft_produtos`
- `workspace.bronze.ft_vendedores`
- `workspace.bronze.dm_categoria_produtos_traducao`
- `workspace.bronze.dm_cotacao_dolar`

2) `Atividade2_SilverLayer.ipynb` (Silver)

Para cada tabela Bronze, foi aplicado um conjunto de transformações visando obter tabelas limpas e padronizadas na camada Silver (`workspace.silver.*`). Principais ações:

- `ft_consumidores`: seleção e renomeação de colunas, normalização de texto (uppercase), deduplicação por `id_consumidor` usando Last-In-Wins com `ingestion_timestamp` (window + row_number). Resultado persistido como `silver.ft_consumidores`.

- `ft_pedidos`: mapeamento e cast de timestamps, tradução dos valores de status para PT (ex.: `delivered` -> `entregue`), cálculo de métricas de prazo (tempo entre compra/entrega, diferença para estimativa) e flag `entrega_no_prazo`. Persistido como `silver.ft_pedidos`.

- `ft_itens_pedidos`: cast de colunas numéricas (`order_item_id` -> int; `price`, `freight_value` -> decimal), renomeação de colunas. Persistido como `silver.ft_itens_pedidos`.

- `ft_pagamentos_pedidos`: tradução/padronização de `payment_type` para rótulos em PT, cast de `payment_installments` e `payment_value`. Persistido como `silver.ft_pagamentos_pedidos`.

- `ft_avaliacoes_pedidos`: conversão segura de timestamps (`try_cast` via `expr`), filtro de registros inválidos (sem `order_id` ou com datas futuras), renomeação de campos e persistência em `silver.ft_avaliacoes_pedidos`.

- `ft_produtos`: padronização de dimensões físicas (casts para int) e renomeação de colunas. Persistido como `silver.ft_produtos`.

- `ft_vendedores`: renomeação e normalização de cidade/estado (uppercase). Persistido como `silver.ft_vendedores`.

- `dm_categoria_produtos_traducao`: seleção e padronização de colunas de tradução (`nome_produto_pt`, `nome_produto_en`). Persistido como `silver.dm_categoria_produtos_traducao`.

- `dm_cotacao_dolar`: criado calendário entre a data mínima e máxima das cotações e aplicação de forward-fill (ultimo valor conhecido) para preencher dias sem cotação; resultado persistido em `silver.dm_cotacao_dolar`.

- Validações e limpeza referencial: checks para identificar registros órfãos (pedidos sem consumidor, itens sem pedido) e limpeza (left_semi) quando necessário, sobrescrevendo as tabelas Silver envolvidas.

- `ft_pedido_total`: agregado de pagamentos por pedido (soma `valor_pagamento`), join com pedidos (extrai `data_pedido`) e conversão BRL->USD usando a cotação do dia do pedido (campo `cotacao_dolar`), persistido como `silver.ft_pedido_total`.

---

## Como executar (visão geral)

Requisitos

- Ambiente com Spark 3.x e Delta Lake configurado (o notebook assume disponibilidade de `spark.table` e escrita via `saveAsTable`).
- Internet (opcional) para a extração das cotações via API do Banco Central (usado no notebook Bronze).

Execução recomendada

1. Abra `notebooks/Atividade2_BronzeLayer.ipynb` e ajuste o caminho `path_files` para apontar para a pasta `dados/` ou para o local onde os CSVs estão armazenados.
2. Execute as células do notebook Bronze para gerar as tabelas `workspace.bronze.*` e a tabela de cotações (se desejado).
3. Abra `notebooks/Atividade2_SilverLayer.ipynb` e execute as células sequencialmente — cada bloco carrega uma tabela Bronze, aplica transformações e persiste na camada Silver (`workspace.silver.*`).

Observações de execução

- Muitos blocos escrevem em modo `overwrite` nas tabelas Delta — tenha cuidado se quiser preservar versões anteriores; você pode alterar o modo de gravação para `append` ou usar versionamento do Delta (Time Travel) conforme apropriado.
- Para reproduzir localmente sem Delta, os trechos de persistência podem ser adaptados para escrever em Parquet/CSV.

---

## Estrutura de saída (tabelas Silver)

- `workspace.silver.ft_consumidores`
- `workspace.silver.ft_pedidos`
- `workspace.silver.ft_itens_pedidos`
- `workspace.silver.ft_pagamentos_pedidos`
- `workspace.silver.ft_avaliacoes_pedidos`
- `workspace.silver.ft_produtos`
- `workspace.silver.ft_vendedores`
- `workspace.silver.dm_categoria_produtos_traducao`
- `workspace.silver.dm_cotacao_dolar`
- `workspace.silver.ft_pedido_total`

---

## Observações finais e sugestões

- Os notebooks foram organizados para serem educativos e reproduzíveis; os comentários e markdowns foram melhorados para explicar a intenção de cada bloco (carregamento, transformação, persistência, validação).
- Próximos passos recomendados:
  - Adicionar testes/asserções automatizadas após cada transformação (ex.: counts, null checks, constraints simples);
  - Extrair transformações repetitivas em funções reutilizáveis (módulo `.py`) e criar um orchestrator (Airflow, Prefect ou jobs Databricks) para agendamento;
  - Implementar retenção/particionamento das tabelas Silver para reduzir custo de leitura/escrita.