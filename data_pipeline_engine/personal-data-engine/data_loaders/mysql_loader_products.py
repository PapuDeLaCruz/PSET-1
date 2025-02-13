from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.mysql import MySQL
from pandas import DataFrame
from os import path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

#use una plantilla que tenian los de mage ai mismo
@data_loader
def load_data_from_mysql(*args, **kwargs) -> dict[str, DataFrame]:
    """
    Template for loading data from a MySQL database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: /design/data-loading#mysql
    """
    #una tabla para poder medio que automatizar la creacion de los diccionarios de dfs
    tablas = ["aisles", "departments", "instacart_orders", "order_products", "products"]
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    
    dataframes = {}

    #extrayendo las tabls de mi sql, haciendolas df y agregandolas como valor a mis diccionarios
    with MySQL.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        for table in tablas:
            query = f'SELECT * FROM {table}'
            dataframes[table] = loader.load(query)

    #regresando una lista de diccionarios para ser exportados
    return dataframes
