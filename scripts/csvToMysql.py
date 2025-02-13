import mysql.connector
import os
import pandas as pd
from sqlalchemy import (create_engine, Table, Column, Integer, String, Float, ForeignKey, MetaData)


#agregar la pk de la tabla que me hizo problemas (la de instacart_orders)
#agregar las fks de la tabla que me hizo problemas (la de order_products)

def connect(db_password):

    global connection, cursor
    connection = mysql.connector.connect(
        host="localhost",
        user="pipeline",      
        password=db_password 
    )
    cursor = connection.cursor()
    return connection,cursor

def alchemy_connect(db_password):
    host="localhost"
    username="pipeline"    
    password=db_password
    port=3306
    database="instacart_db"


    connection_string = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"

    engine = create_engine(connection_string)

    return engine

def createDatabase(cursor):
    cursor.execute("CREATE DATABASE IF NOT EXISTS instacart_db;")
    cursor.execute("USE instacart_db;")

def alchemy_create_table(csv_filenames, engine):
    print(csv_filenames)
    metadata = MetaData()

    tables_definitions = {
        'aisles': [
            Column('aisle_id', Integer, primary_key=True),
            Column('aisle', String(255))
        ],
        'departments': [
            Column('department_id', Integer, primary_key=True),
            Column('department', String(255))
        ],
        'instacart_orders': [
            Column('order_id', Integer),
            Column('user_id', Integer),
            Column('order_number', Integer),
            Column('order_dow', Integer),
            Column('order_hour_of_day', Integer),
            Column('days_since_prior_order', Float)
        ],
        'order_products': [
            Column('order_id', Integer),
            Column('product_id', Integer),
            Column('add_to_cart_order', Integer),
            Column('reordered', Integer)
        ],
        'products': [
            Column('product_id', Integer, primary_key=True),
            Column('product_name', String(255)),
            Column('aisle_id', Integer, ForeignKey('aisles.aisle_id')),
            Column('department_id', Integer, ForeignKey('departments.department_id'))
        ]
    }

    tables = {}
    for table_name, cols in tables_definitions.items():
        tables[table_name] = Table(table_name, metadata, *cols)

    metadata.create_all(engine)

def alchemy_fill_table(table_name, csv_file, engine):
    print(csv_file)
    df = pd.read_csv(csv_file, delimiter=';', na_values= None)

    df.to_sql(
    name=table_name,
    con=engine,
    if_exists='append',
    index=False,
    method='multi',
    chunksize=1000
    )

def dropAllIf(cursor):

    cursor.execute("SHOW DATABASES;")
    databases = cursor.fetchall()

    if ("instacart_db",) in databases:
        cursor.execute("DROP DATABASE IF EXISTS instacart_db;")
        print("Database 'instacart_db' dropped.")

def disconnect(connection,cursor):
    if cursor:
        cursor.close()
    if connection:
        connection.close()
    print("Disconnected from the database.")

def getCSV():
    directory = '../data'
    csv_filenames = []
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            csv_filenames.append(os.path.join(directory, filename))
    csv_filenames.sort()
    csv_filenames.append(csv_filenames[3])
    csv_filenames[3]= csv_filenames[4]
    csv_filenames[4]= csv_filenames[5]
    csv_filenames.pop()
    return csv_filenames

def alchemy_create_and_insert_loop(csv_filenames, engine): 
    alchemy_create_table(csv_filenames, engine)
    for csv_filename in csv_filenames:
        table_name = csv_filename.split("/")[-1].split(".")[0]
        alchemy_fill_table(table_name, csv_filename, engine)

def main():
    try:
        db_password = os.getenv('DB_PASSWORD')
        connection,cursor = connect(db_password)
        dropAllIf(cursor)
        createDatabase(cursor)
        alchemy_create_and_insert_loop(getCSV(), alchemy_connect(db_password))
        
    finally:
        disconnect(connection, cursor)

if __name__ == "__main__":
    main()