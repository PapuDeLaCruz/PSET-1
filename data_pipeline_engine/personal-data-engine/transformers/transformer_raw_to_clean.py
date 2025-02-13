from mage_ai.data_cleaner.transformer_actions.constants import ImputationStrategy
from mage_ai.data_cleaner.transformer_actions.base import BaseAction
from mage_ai.data_cleaner.transformer_actions.constants import ActionType, Axis
from mage_ai.data_cleaner.transformer_actions.utils import build_transformer_action
import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def execute_transformer_action(dataframes_iniciales: dict[str, pd.DataFrame], **kwargs) -> dict[str, pd.DataFrame]:
    """
    Execute Transformer Action: ActionType.IMPUTE
    Docs: https://docs.mage.ai/guides/transformer-blocks#fill-in-missing-values
    """
    tables = ["fct_orders", "dim_products"]
    dataframes = {}

    #lidiando con valores nulos, rellenando o eliminadolos.
    for name, df in dataframes_iniciales.items():
        if name == "instacart_orders":
            df = df.fillna(0)
            df = df.drop_duplicates()
        elif name == "order_products":
            df = df.dropna()
            #se que dijiste que pongamos -1 pero la verdad no encontre el patron, entonces no me parecia justo
        elif name == "products":
            df = df.fillna("Sin Nombre")
        dataframes_iniciales[name] = df

    #aplicando la transformacion que esta descrita en el eda
    df_products = dataframes_iniciales.get("products")
    df_aisles = dataframes_iniciales.get("aisles")
    df_departments = dataframes_iniciales.get("departments")
    df_order_products = dataframes_iniciales.get("order_products")
    df_instacart_orders = dataframes_iniciales.get("instacart_orders")


    df_dim_products_beta = pd.merge(df_products, df_aisles, on="aisle_id", how="inner")
    df_dim_products_beta = pd.merge(df_dim_products_beta, df_departments, on="department_id", how="inner")
    df_dim_products_beta.drop(df_dim_products_beta.columns[[2, 3]], axis=1, inplace=True)
    df_dim_products_beta.rename(columns={'product_id': 'dim_product_id'}, inplace=True)

    df_fct_products = df_order_products.copy()
    df_fct_products.rename(columns={'product_id': 'dim_product_id', 'order_id': 'fct_order_id'}, inplace=True)

    df_dim_orders = df_instacart_orders.copy()
    df_dim_orders['dim_order_id'] = range(1, len(df_dim_orders) + 1)
    df_dim_orders.rename(columns={'order_id': 'fct_order_id'}, inplace=True)
    df_dim_orders.drop(df_dim_orders.columns[[1, 2, 5]], axis=1, inplace=True)
    df_dim_orders = df_dim_orders[[df_dim_orders.columns[-1]] + list(df_dim_orders.columns[:-1])]

    df_fct_orders_beta = df_instacart_orders.copy()
    df_fct_orders_beta.rename(columns={'order_id': 'fct_order_id'}, inplace=True)
    df_fct_orders_beta.drop(df_fct_orders_beta.columns[[3, 4]], axis=1, inplace=True)

    df_fct_orders = pd.merge(df_fct_products, df_fct_orders_beta, on="fct_order_id", how="inner")

    df_dim_products_alfa = pd.merge(df_fct_products, df_dim_orders, on="fct_order_id", how="inner")
    df_dim_products = pd.merge(df_dim_products_beta, df_dim_products_alfa, on="dim_product_id", how="inner")
    df_dim_products = df_dim_products[list(df_dim_products.columns)[:1] + [list(df_dim_products.columns)[4]] + list(df_dim_products.columns)[1:4] +          list(df_dim_products.columns)[5:]]
    df_dim_products.drop(df_dim_products.columns[[5, 6, 7]], axis=1, inplace=True) 

    dataframes[tables[0]] = df_fct_orders
    dataframes[tables[1]] = df_dim_products

    #regresando un diccionario de nombres y dfs
    return dataframes
