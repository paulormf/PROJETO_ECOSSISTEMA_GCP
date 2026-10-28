# Auditoria e change log da origem

## Objetivo

Registrar as alterações das tabelas do schema `ecommerce` em uma tabela central de auditoria, permitindo que o extrator Python leia as mudanças por um identificador crescente, sem depender de `atualizado_em` em cada tabela.

## Decisão

A tabela de auditoria será criada no PostgreSQL de origem. A função `audit.capture_change()` será associada às 13 tabelas monitoradas e registrará cada `INSERT`, `UPDATE` e `DELETE`. Os `INSERTs` também serão registrados para manter um único fluxo incremental e um único watermark global para o extrator Python.

Embora a regra atual informe que nenhum registro será excluído, o evento `DELETE` será registrado como proteção operacional. O extrator poderá alertar quando encontrar esse tipo de evento.

## Contrato do evento

| Campo | Função |
|---|---|
| `change_id` | Watermark global crescente usado pelo extrator |
| `occurred_at` | Instante da alteração no banco |
| `operation` | Tipo da operação: `I`, `U` ou `D` |
| `source_schema` | Schema da tabela alterada |
| `source_table` | Tabela alterada |
| `primary_key` | Chave do registro em formato JSONB |
| `old_row` | Estado anterior; nulo para insert |
| `new_row` | Estado posterior; nulo para delete |
| `source_user` | Usuário PostgreSQL que executou a alteração |
| `transaction_id` | Identificador da transação de origem |
| `request_id` | Correlação opcional com a aplicação |
| `processed_at` | Controle opcional de processamento |

## Regras de processamento

1. O extrator lê eventos com `change_id > último_change_id`.
2. O lote é escrito na landing antes de avançar o checkpoint.
3. O checkpoint só é atualizado após a publicação bem-sucedida.
4. O extrator deve ser idempotente para permitir reprocessamento.
5. Eventos `INSERT` e `UPDATE` devem carregar o estado novo completo para facilitar a reconstrução da Bronze.
6. Eventos `DELETE` devem gerar alerta, pois não são esperados no contrato atual.
7. A retenção do change log deve ser maior que a maior janela esperada de indisponibilidade do extrator.

## Decisões ainda necessárias

- Aplicar triggers inicialmente às 13 tabelas identificadas no inventário; novas tabelas deverão entrar mediante revisão.
- Validar em desenvolvimento se o usuário da aplicação possui permissão para inserir no `audit.change_log`.
- Definir o período de retenção do change log.
- Definir se o estado completo da linha será armazenado em `old_row` e `new_row` ou somente o estado novo.
- Definir como a aplicação preencherá `request_id`.
- Confirmar se o usuário da aplicação possui permissão para executar a função de auditoria.
- Criar índices adicionais somente após medir o volume de eventos.
