"""Políticas físicas e chaves das tabelas Bronze."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class BronzeTablePolicy:
    """Configuração física e de identificação de uma tabela Bronze."""

    partition_field: str | None
    clustering_fields: tuple[str, ...]
    primary_key_fields: tuple[str, ...]


BRONZE_POLICIES: dict[str, BronzeTablePolicy] = {
    "armazens": BronzeTablePolicy(None, ("armazem_id",), ("armazem_id",)),
    "categorias": BronzeTablePolicy(None, ("categoria_id",), ("categoria_id",)),
    "clientes": BronzeTablePolicy(
        None,
        ("cliente_id", "status_cliente"),
        ("cliente_id",),
    ),
    "enderecos_clientes": BronzeTablePolicy(
        None,
        ("cliente_id", "endereco_id"),
        ("endereco_id",),
    ),
    "envios": BronzeTablePolicy(
        "criado_em",
        ("pedido_id", "status_envio"),
        ("envio_id",),
    ),
    "estoque": BronzeTablePolicy(
        "atualizado_em",
        ("armazem_id", "variante_id"),
        ("armazem_id", "variante_id"),
    ),
    "eventos_clientes": BronzeTablePolicy(
        "ocorrido_em",
        ("cliente_id", "produto_id", "tipo_evento"),
        ("evento_id",),
    ),
    "itens_pedido": BronzeTablePolicy(
        None,
        ("pedido_id", "variante_id"),
        ("item_pedido_id",),
    ),
    "marcas": BronzeTablePolicy(None, ("marca_id",), ("marca_id",)),
    "pagamentos": BronzeTablePolicy(
        "criado_em",
        ("pedido_id", "status_pagamento"),
        ("pagamento_id",),
    ),
    "pedidos": BronzeTablePolicy(
        "realizado_em",
        ("cliente_id", "status_pedido"),
        ("pedido_id",),
    ),
    "produtos": BronzeTablePolicy(
        None,
        ("marca_id", "categoria_id"),
        ("produto_id",),
    ),
    "variantes_produtos": BronzeTablePolicy(
        None,
        ("produto_id", "ativo"),
        ("variante_id",),
    ),
}


def get_bronze_policy(source_table: str) -> BronzeTablePolicy:
    """Retorna a política da tabela ou falha para evitar carga sem contrato."""

    try:
        return BRONZE_POLICIES[source_table]
    except KeyError as error:
        raise KeyError(
            f"Não existe política Bronze para a tabela: {source_table}"
        ) from error
