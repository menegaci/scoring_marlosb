import datetime
from dateutil.relativedelta import relativedelta
import os

import sqlalchemy
from tqdm import tqdm

def get_abt(DATA_PATH, QUERY_PATH, primeira_safra, ultima_safra):
    ''' Function to create ABT for source tables
    '''
    with open( QUERY_PATH, 'r' ) as open_file:
        query = open_file.read()

    primeira_safra_datetime = datetime.datetime.strptime(primeira_safra,
                                                         "%Y-%m-%d")
    ultima_safra_datetime = datetime.datetime.strptime(ultima_safra, 
                                                       "%Y-%m-%d")

    con = sqlalchemy.create_engine( 'sqlite:///' + DATA_PATH )

    def exec_etl(queries, con):
        for q in tqdm(queries.split(";")[:-1]):
            con.execute(q)

    while primeira_safra_datetime <= ultima_safra_datetime:
        safra = primeira_safra_datetime.strftime("%Y-%m-%d")
        query_exec = query.format( data_ref=safra )
        primeira_safra_datetime = (primeira_safra_datetime
                                   + relativedelta(months=+1))

        if safra == primeira_safra:
            query_exec += f'''DROP TABLE IF EXISTS TB_ABT;
                                    CREATE TABLE TB_ABT AS
                                    SELECT '{safra}' as dt_ref,
                                    *
                                    FROM vw_olist_abt_p2;'''
        else:
            query_exec += f'''INSERT INTO TB_ABT
                                    SELECT '{safra}' as dt_ref,
                                    *
                                    FROM vw_olist_abt_p2;'''

        print(safra)
        exec_etl(query_exec, con)