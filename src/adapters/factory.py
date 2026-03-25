from adapters.base import BaseWarehouseAdapter


def get_adapter(cfg) -> BaseWarehouseAdapter:
    """
    Returns the correct warehouse adapter based on WAREHOUSE_TYPE config.
    """
    warehouse_type = cfg.WAREHOUSE_TYPE.lower()

    if warehouse_type == "snowflake":
        from adapters.snowflake_adapter import SnowflakeAdapter
        return SnowflakeAdapter(cfg)

    raise ValueError(
        f"Unsupported WAREHOUSE_TYPE: '{cfg.WAREHOUSE_TYPE}'. "
        "Supported values: snowflake"
    )
