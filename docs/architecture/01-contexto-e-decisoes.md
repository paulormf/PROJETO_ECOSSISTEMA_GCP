# Contexto e decisões iniciais

## Objetivo

Construir uma plataforma de engenharia de dados para produção no Google Cloud, com origem em PostgreSQL, arquitetura medalhão e ambientes separados de desenvolvimento, homologação e produção.

## Escopo inicial conhecido

- Origem: PostgreSQL.
- Quantidade informada inicialmente: 22 tabelas; 13 foram identificadas no primeiro inventário recebido.
- Tabelas identificadas: `pedidos`, `itens_pedido`, `clientes`, `eventos_clientes`, `pagamentos`, `envios`, `estoque`, `enderecos_clientes`, `variantes_produtos`, `produtos`, `marcas`, `categorias` e `armazens`.
- Extração incremental: baseada em watermark de atualização, que deverá existir em todas as tabelas ingeridas.
- Volume conhecido das 13 tabelas listadas: aproximadamente 3,6 GB de tamanho total.
- Frequência analítica desejada: horária.
- Região GCP preferida: `us-central1`.
- Repositório: GitHub.
- Infraestrutura como código: Terraform.
- CI/CD: GitHub Actions.
- Dados sensíveis sujeitos a requisitos de LGPD.
- Datastream: fora do escopo desta fase.
- Change log: registrará `INSERT` e `UPDATE` em todas as tabelas monitoradas, usando `change_id` como watermark global.

## Arquitetura adotada nesta fase

```text
PostgreSQL
    |
    | Extração inicial e incremental por watermark
    v
Python
    |
    | Arquivos imutáveis de ingestão
    v
Cloud Storage - landing
    |
    | Carga e controle de ingestão
    v
BigQuery - Bronze
    |
    | Transformações e qualidade
    v
BigQuery - Silver
    |
    | Modelo analítico
    v
BigQuery - Gold
```

O Python será responsável pela conexão com PostgreSQL, leitura incremental, serialização dos lotes, escrita no Cloud Storage e registro do estado de cada execução. O mecanismo de execução e orquestração será decidido em uma etapa posterior, considerando Cloud Run Jobs, Composer ou outra opção adequada.

## Estratégia de incrementalidade

Nesta fase, não assumiremos CDC nativo do PostgreSQL. A extração será baseada em watermark:

1. Ler o último watermark confirmado para a tabela.
2. Buscar registros posteriores ao watermark.
3. Persistir o lote em uma área temporária.
4. Validar o lote.
5. Publicar o arquivo na landing.
6. Atualizar o watermark somente após a publicação bem-sucedida.

O watermark deverá ser monotônico e, preferencialmente, acompanhado por uma chave primária como desempate. Exemplos de colunas candidatas são `updated_at`, `modified_at`, `created_at` ou uma sequência crescente.

### Limitações que precisam ser tratadas

- Um watermark de criação não detecta atualizações.
- Não haverá exclusões físicas no escopo informado; portanto, o extrator não precisará processar deletes nesta fase.
- Registros com o mesmo timestamp exigem uma chave de desempate.
- Atrasos de commit podem exigir uma janela de sobreposição.
- Alterações posteriores à inserção exigem `atualizado_em`, sequência de versão ou tabela de auditoria.

Essas limitações serão verificadas tabela por tabela no inventário da origem.

## Ambientes

Os ambientes serão isolados por projetos GCP distintos:

- `dev`: desenvolvimento e testes individuais.
- `hom`: homologação e testes integrados.
- `prod`: dados e cargas oficiais.

Os identificadores finais dos projetos ainda estão pendentes.

## Segurança e LGPD

- Segredos não serão armazenados no Git.
- Credenciais serão gerenciadas pelo Secret Manager.
- O usuário PostgreSQL usado pela ingestão terá somente permissões de leitura necessárias.
- A camada Bronze terá acesso restrito.
- A camada Bronze preservará os dados brutos da origem, sem mascaramento.
- O acesso à Bronze será limitado por IAM, ambiente e função operacional.
- Colunas sensíveis deverão ser classificadas antes da publicação da Silver.
- O mascaramento, a tokenização ou a redução de dados sensíveis ocorrerá na Silver.
- A Gold deverá expor somente os atributos necessários aos consumidores.
- Permissões deverão ser separadas por ambiente e função.
- Os arquivos da landing deverão possuir política de retenção definida.

## Decisões pendentes

| Tema | Estado | Próxima informação necessária |
|---|---|---|
| Host e banco PostgreSQL | Pendente | Informar tipo de hospedagem, host e banco |
| Conectividade | Pendente | Definir IP allowlist, VPN ou conectividade privada |
| Tabelas e colunas de watermark | Bloqueador técnico | Criar ou confirmar watermark de atualização em todas as tabelas |
| Tratamento de exclusões | Resolvido provisoriamente | Não haverá exclusões físicas |
| Projeto GCP de cada ambiente | Pendente | Informar ou definir IDs |
| Volume inicial | Parcialmente conhecido | Confirmar as 9 tabelas restantes |
| Volume mensal de alterações | Pendente | Estimar ou medir alterações |
| Campos sensíveis | Pendente | Listar colunas sujeitas à LGPD |
| Retenção | Pendente | Definir retenção operacional e analítica |
| Orquestração | Pendente | Comparar Cloud Run Jobs, Composer e alternativas |

## Critérios para avançar

Antes de criar o extrator Python, devemos confirmar:

1. As tabelas possuem watermark confiável.
2. O contrato de atualização da origem está definido.
3. O usuário de ingestão possui acesso somente leitura.
4. A conectividade do ambiente de desenvolvimento foi validada.
5. Os volumes permitem definir tamanho de lote e custo.
6. A classificação dos dados sensíveis foi iniciada.
