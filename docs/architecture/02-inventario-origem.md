# Inventário da origem PostgreSQL

Este documento será preenchido antes da implementação do extrator Python. Nenhuma senha, token ou string de conexão deve ser registrada no repositório.

## Identificação da origem

| Item | Valor |
|---|---|
| Tipo | PostgreSQL |
| Hospedagem | Pendente |
| Servidor/FQDN | Pendente |
| Porta | Pendente |
| Banco de dados | Pendente |
| Versão | Pendente |
| Região da origem | Pendente |
| Total informado inicialmente | 22 |
| Tabelas identificadas no inventário recebido | 13 |
| Volume total aproximado das tabelas listadas | 3,6 GB |
| CDC nativo utilizado nesta fase | Não |
| Estratégia incremental | Watermark |

## Decisao vigente sobre enderecos

O cadastro de um novo endereco gera um novo `endereco_id`. A alteracao de um
endereco existente mantem o mesmo `endereco_id`.

Consequentemente:

- `INSERT` representa um novo endereco;
- `UPDATE` representa uma nova versao do mesmo endereco no change log;
- `old_row` e `new_row` preservam o historico da alteracao;
- a Bronze mantem a situacao operacional atual;
- uma futura dimensao SCD Tipo 2 podera reconstruir as versoes historicas a
  partir de `change_id` e `occurred_at`.

Essa regra nao altera o primeiro escopo da Gold, que nao inclui uma dimensao de
enderecos nem uma fato adicional.

## Tabelas selecionadas

Preencher uma linha por tabela candidata à ingestão.

| Schema | Tabela | PK | Coluna watermark | Tipo | Atualiza em UPDATE? | Exclusão | Linhas aprox. | Tamanho aprox. | Dados LGPD |
|---|---|---|---|---|---|---|---:|---:|---|
| ecommerce | armazens | `armazem_id` | `criado_em` | timestamp | Insuficiente; todos os registros podem alterar | Não haverá exclusão física | 5 | 24 KB | Não identificado |
| ecommerce | categorias | `categoria_id` | `criado_em` | timestamp | Insuficiente; todos os registros podem alterar | Não haverá exclusão física | 100 | 24 KB | Não identificado |
| ecommerce | clientes | `cliente_id` | `atualizado_em` | timestamp | Candidato válido; confirmar manutenção | Não haverá exclusão física | 200.000 | 45 MB | Sim |
| ecommerce | enderecos_clientes | `endereco_id` | `criado_em` | timestamp | Insuficiente; histórico precisa ser preservado | Não haverá exclusão física | 200.000 | 37 MB | Sim |
| ecommerce | envios | `envio_id` | `criado_em` | timestamp | Insuficiente; todos os registros podem alterar | Não haverá exclusão física | 1.000.000 | 180 MB | A confirmar |
| ecommerce | estoque | (`armazem_id`, `variante_id`) | `atualizado_em` | timestamp | Candidato válido; confirmar manutenção | Não haverá exclusão física | 500.000 | 44 MB | Não identificado |
| ecommerce | eventos_clientes | `evento_id` | `ocorrido_em` ou `evento_id` | timestamp/bigint | `evento_id` captura inserções, mas não atualizações | Não haverá exclusão física | 10.000.000 | 2.356 MB | A confirmar |
| ecommerce | itens_pedido | `item_pedido_id` | Nenhuma identificada | — | Insuficiente; todos os registros podem alterar | Não haverá exclusão física | 2.999.995 | 368 MB | A confirmar |
| ecommerce | marcas | `marca_id` | `criado_em` | timestamp | Insuficiente; todos os registros podem alterar | Não haverá exclusão física | 500 | 120 KB | Não identificado |
| ecommerce | pagamentos | `pagamento_id` | `criado_em` | timestamp | Insuficiente; todos os registros podem alterar | Não haverá exclusão física | 1.000.000 | 238 MB | A confirmar |
| ecommerce | pedidos | `pedido_id` | `atualizado_em` | timestamp | Candidato válido; confirmar manutenção | Não haverá exclusão física | 999.559 | 349 MB | A confirmar |
| ecommerce | produtos | `produto_id` | `atualizado_em` | timestamp | Candidato válido; confirmar manutenção | Não haverá exclusão física | 100.000 | 16 MB | A confirmar |
| ecommerce | variantes_produtos | `variante_id` | `atualizado_em` | timestamp | Candidato válido; confirmar manutenção | Não haverá exclusão física | 100.000 | 18 MB | A confirmar |

As tabelas `orders`, `order_items` e `customers` usadas nos exemplos iniciais não aparecem com esses nomes no inventário recebido. O modelo real identificado utiliza nomes em português.

## Avaliação do watermark

| Schema | Tabela | Watermark escolhido | Chave de desempate | Janela de sobreposição | Estratégia de exclusão | Observações |
|---|---|---|---|---|---|---|
| ecommerce | clientes | `atualizado_em` | `cliente_id` | Pendente | Pendente | Watermark explícito identificado |
| ecommerce | estoque | `atualizado_em` | (`armazem_id`, `variante_id`) | Pendente | Pendente | PK composta |
| ecommerce | pedidos | `atualizado_em` | `pedido_id` | Pendente | Pendente | Watermark explícito identificado |
| ecommerce | produtos | `atualizado_em` | `produto_id` | Pendente | Pendente | Watermark explícito identificado |
| ecommerce | variantes_produtos | `atualizado_em` | `variante_id` | Pendente | Pendente | Watermark explícito identificado |
| ecommerce | eventos_clientes | `evento_id` ou `ocorrido_em` | `evento_id` | Pendente | Pendente | Confirmar se é append-only |

### Tabelas sem watermark de atualização identificado

`armazens`, `categorias`, `enderecos_clientes`, `envios`, `itens_pedido`, `marcas` e `pagamentos` não apresentam uma coluna `atualizado_em`, `updated_at` ou equivalente no inventário recebido. Como todos os registros podem alterar, essas tabelas precisam receber um watermark, uma versão monotônica ou uma tabela de auditoria antes da ingestão incremental.

`envios` e `pagamentos` possuem colunas de status, portanto não devem ser tratados como imutáveis sem um watermark de atualização.

## Estados de negócio identificados

Foram encontradas as seguintes colunas de estado:

| Tabela | Coluna | Interpretação atual |
|---|---|---|
| clientes | `status_cliente` | Estado de negócio; não caracteriza exclusão lógica por si só |
| envios | `status_envio` | Estado operacional; pode mudar após a criação |
| pagamentos | `status_pagamento` | Estado financeiro; pode mudar após a criação |
| pedidos | `status_pedido` | Estado do pedido; pode mudar após a criação |
| produtos | `status_produto` | Estado de catálogo; confirmar se representa inativação |
| variantes_produtos | `ativo` | Indicador de atividade; não necessariamente exclusão lógica |

Essas colunas devem ser capturadas como dados de negócio. Elas não serão usadas como indicador de delete. Foi informado que não haverá exclusões físicas nesta origem.

### Requisitos mínimos

- A coluna deve permitir ordenar os registros de forma consistente.
- Para atualizações, a coluna precisa ser alterada em cada mudança relevante.
- Para timestamps, deve existir uma estratégia para empates e atrasos de commit.
- O estado do watermark deve ser atualizado somente após o lote ser publicado com sucesso.
- A consulta incremental deve ser idempotente ou suportar reprocessamento seguro.

## Decisão sobre a origem

Foi informado que todos os registros podem ser alterados após a inserção e que nenhum registro será excluído. Portanto, `criado_em` não é um watermark válido para essas tabelas. O fato de `eventos_clientes.evento_id` ser crescente permite capturar novas inserções, mas não detecta alterações em eventos já existentes.

Antes do extrator Python, a origem deverá fornecer uma destas alternativas:

1. Adicionar `atualizado_em TIMESTAMPTZ NOT NULL` a todas as tabelas ingeridas e atualizá-lo por aplicação ou trigger.
2. Adicionar uma coluna de versão monotônica atualizada a cada alteração.
3. Criar uma tabela de auditoria/change log com chave, operação e instante da alteração.

A alternativa recomendada é `atualizado_em` com índice composto por `(atualizado_em, chave_primaria)`, mantendo uma janela de sobreposição no extrator. Para `estoque`, a chave de desempate deverá considerar a chave composta.

### Endereços com histórico

O histórico de `enderecos_clientes` deverá ser preservado na Bronze. Ainda precisamos confirmar se o histórico é implementado por novas versões de registro ou por UPDATE no mesmo registro. Se houver novas versões, será necessário identificar a coluna de validade ou versão para modelar corretamente a Silver.

## Classificação de dados sensíveis

| Schema | Tabela | Coluna | Categoria | Tratamento na Silver | Expor na Gold? |
|---|---|---|---|---|---|
| ecommerce | clientes | `email` | Contato/PII | Pendente | Pendente |
| ecommerce | clientes | `telefone` | Contato/PII | Pendente | Pendente |
| ecommerce | clientes | `data_nascimento` | Dado pessoal | Pendente | Pendente |
| ecommerce | enderecos_clientes | `nome_destinatario` | Identificação/endereço | Pendente | Pendente |
| ecommerce | enderecos_clientes | `cep`, `rua`, `numero`, `complemento`, `bairro`, `cidade`, `estado`, `pais` | Endereço | Pendente | Pendente |
| ecommerce | eventos_clientes | `dados_evento` | A confirmar; JSONB pode conter PII | Inspecionar contrato do evento | Pendente |

Categorias sugeridas: identificador pessoal, contato, endereço, financeiro, autenticação, saúde, localização ou não sensível.

## Consultas de diagnóstico

Executar no PostgreSQL usando uma conta de leitura autorizada. Os resultados devem ser revisados e então resumidos neste documento. Não registrar credenciais.

### Versão e informações do banco

```sql
SELECT
    current_database() AS database_name,
    current_user AS connected_user,
    version() AS postgres_version;
```

### Tabelas, linhas estimadas e tamanho

```sql
SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    c.reltuples::bigint AS estimated_rows,
    pg_size_pretty(pg_table_size(c.oid)) AS table_size,
    pg_size_pretty(pg_indexes_size(c.oid)) AS indexes_size,
    pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size
FROM pg_class AS c
JOIN pg_namespace AS n
    ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
ORDER BY pg_total_relation_size(c.oid) DESC;
```

### Chaves primárias

```sql
SELECT
    ns.nspname AS schema_name,
    cls.relname AS table_name,
    att.attname AS column_name,
    key_columns.ordinality AS key_ordinal
FROM pg_constraint AS con
JOIN pg_class AS cls
    ON cls.oid = con.conrelid
JOIN pg_namespace AS ns
    ON ns.oid = cls.relnamespace
JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS key_columns(attnum, ordinality)
    ON TRUE
JOIN pg_attribute AS att
    ON att.attrelid = cls.oid
   AND att.attnum = key_columns.attnum
WHERE con.contype = 'p'
ORDER BY ns.nspname, cls.relname, key_columns.ordinality;
```

### Colunas e tipos

```sql
SELECT
    table_schema,
    table_name,
    ordinal_position,
    column_name,
    data_type,
    udt_name,
    is_nullable
FROM information_schema.columns
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY table_schema, table_name, ordinal_position;
```

### Candidatas a watermark

Esta consulta é apenas um filtro inicial. A confirmação depende do comportamento real da aplicação.

```sql
SELECT
    table_schema,
    table_name,
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
  AND (
      LOWER(column_name) LIKE '%updated%'
      OR LOWER(column_name) LIKE '%modified%'
      OR LOWER(column_name) LIKE '%changed%'
      OR LOWER(column_name) LIKE '%created%'
      OR LOWER(column_name) LIKE '%timestamp%'
      OR LOWER(column_name) LIKE '%sequence%'
      OR LOWER(column_name) LIKE '%version%'
  )
ORDER BY table_schema, table_name, ordinal_position;
```

### Colunas candidatas a dados sensíveis

Esta consulta é apenas um ponto de partida. A classificação final depende do significado de negócio.

```sql
SELECT
    table_schema,
    table_name,
    ordinal_position,
    column_name,
    data_type
FROM information_schema.columns
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
  AND (
      LOWER(column_name) LIKE '%cpf%'
      OR LOWER(column_name) LIKE '%cnpj%'
      OR LOWER(column_name) LIKE '%email%'
      OR LOWER(column_name) LIKE '%phone%'
      OR LOWER(column_name) LIKE '%telefone%'
      OR LOWER(column_name) LIKE '%address%'
      OR LOWER(column_name) LIKE '%endereco%'
      OR LOWER(column_name) LIKE '%birth%'
      OR LOWER(column_name) LIKE '%nascimento%'
  )
ORDER BY table_schema, table_name, ordinal_position;
```

O filtro automático não é suficiente para este modelo, porque há colunas sensíveis com nomes em português e dados potencialmente sensíveis dentro de `eventos_clientes.dados_evento`. A classificação deve ser revisada pelo responsável de negócio e, quando necessário, por segurança/privacidade.

### Resultado recebido sobre recursos de replicação

O resultado enviado informou `sem resultado` para a verificação opcional. Isso indica que nenhum replication slot foi apresentado, mas ainda não permite concluir o valor de `wal_level`. O valor deve ser obtido separadamente caso a opção de logical decoding seja considerada no futuro.

### Verificação opcional de recursos de replicação

PostgreSQL pode oferecer mecanismos baseados em WAL/logical replication dependendo da versão, hospedagem e configuração. Eles não fazem parte da estratégia desta fase, mas podem ser registrados para decisão futura.

```sql
SHOW wal_level;

SELECT
    slot_name,
    plugin,
    slot_type,
    active,
    restart_lsn,
    confirmed_flush_lsn
FROM pg_replication_slots;
```

## Critérios de validação

- Cada tabela selecionada possui chave primária ou justificativa para ausência. No inventário recebido, todas as 13 tabelas possuem PK; `estoque` possui PK composta.
- Cada tabela possui uma estratégia de watermark documentada. Atualmente isso está confirmado apenas como candidato para cinco tabelas com `atualizado_em`.
- A estratégia identifica atualizações relevantes.
- Não haverá exclusões físicas no escopo informado; essa decisão deve ser mantida como contrato da origem.
- O lote pode ser reprocessado sem duplicação incorreta.
- O volume inicial conhecido, aproximadamente 3,6 GB, será usado para dimensionar o backfill Python.
- O volume de alterações será usado para definir tamanho e frequência dos lotes.
- Dados sensíveis serão classificados antes de qualquer publicação para consumidores.
- Colunas de status serão tratadas como atributos de negócio, não como sinal automático de exclusão.
- Nenhuma tabela será considerada pronta para ingestão incremental até possuir watermark, versão ou change log para alterações posteriores à inserção.
