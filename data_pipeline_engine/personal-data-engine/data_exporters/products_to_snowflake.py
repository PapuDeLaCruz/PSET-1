from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.snowflake import Snowflake
from pandas import DataFrame
from os import path


if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_snowflake(dataframes: dict[str, DataFrame], **kwargs) -> None:
    """
    Template for exporting data to a Snowflake warehouse.
    """
    #usando un template
    #exportando mis datos a snowflake. extraigo la clave del diccionario como el nombre y el contenido como la tabla
    database = 'INSTACART_DB'
    schema = 'RAW'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Snowflake.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
       
        for table_name, df in dataframes.items():

            check_query = f"""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{schema}' 
            AND TABLE_NAME = '{table_name}'
            """            
            result = loader.execute(check_query)
            table_exists = result[0][0] > 0

            #agregue este truncate ya que para hacer drop mi pipeline service tendria que ser owner y no quiero eso.
            #con truncate puedo ejecutar el pipeline multiples veces sin duplicar datos de manera innecesaria
            if table_exists:
                loader.execute(f"TRUNCATE TABLE {database}.{schema}.\"{table_name}\"")

            loader.export(
                df,
                table_name,
                database,
                schema,
                if_exists='append',
            )
