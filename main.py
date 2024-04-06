import os
import connection
import sqlparse
import pandas as pd


if __name__ == '__main__':
    # connection to data source
    conf = connection.config('marketplace_prod')
    conn, engine = connection.get_conn(conf, 'DataSource')
    cursor = conn.cursor()

    # connection dwh
    conf_dwh = connection.config('dwh')
    conn_dwh, engine_dwh = connection.get_conn(conf, 'DataWarehose')
    cursor_dwh = conn_dwh.cursor()

    # get query string
    path_query = os.getcwd()+'/query/'
    query = sqlparse.format(
        open(path_query+'query.sql', 'r').read(), strip_comment=True
    ).strip()
    dwh_design = sqlparse.format(
        open(path_query+'dwh_design.sql', 'r').read(), strip_comment=True
    ).strip()

    
    
    try:
        # get data
        print('[INFO] services etl is runing..')
        df = pd.read_sql(query, engine)
        
        # create schema dwh
        cursor_dwh = conn_dwh.cursor()
        conn_dwh = conn.commit()

        # ingest data to dwh
        df.to_sql(
            'dim_orders',
            engine_dwh,
            schema='vina_dwh',
            if_exists='append', 
            index=False
        )

        print('[INFO] service etl is succes..')
    except Exception as e:
        print('[INFO] services etl is failes')
        print(str(e))