from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.snowflake import Snowflake
import pandas as pd
from os import path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

#basicamente lo mismo que el data loader de mysql, solo que este es de snowflake. Regresa lo mismo.
@data_loader
def load_data_from_snowflake(*args, **kwargs) -> dict[str, pd.DataFrame]:
    """
    Template for loading data from a Snowflake database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: /design/data-loading#snowflake
    """
    tables = ["aisles", "departments", "instacart_orders", "order_products", "products"]
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    dataframes = {}

    #cargando las tablas de snowflake para ser transformadas y luego exportadas
    with Snowflake.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        for table in tables:
            query = f'SELECT * FROM "{table}"'
            dataframes[table] = loader.load(query)

    #regresando un diccionario de dfs
    return dataframes
