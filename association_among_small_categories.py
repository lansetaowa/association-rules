# -*- coding: utf-8 -*-
"""
Created on Fri Sep 13 13:35:28 2024

@author: elisa
"""

from association_rules.Query_Data_From_Postgresql import Query_Data_From_Postgresql
from association_rules.sql_text import *
from association_rules.association_rules import process_data, asso_rule

import numpy as np
import pandas as pd


# load env parameters to connect to DB
# load_dotenv('pg_conn.env')

# DBNAME = os.getenv('DBNAME')
# USER = os.getenv('USER')
# PW = os.getenv('PW')
# HOST = os.getenv('HOST')
# PORT = os.getenv('PORT')

# query_data = Query_Data_From_Postgresql(dbname = DBNAME,
#                                         user = USER,
#                                         pw = PW,
#                                         host = HOST,
#                                         port = PORT)

# receipt_df = query_data.query_data_from_pg(receipt_sql.format('2022-08-01','2022-08-02'))

# for the convenience of illustration without reading from my DB
# receipt_df is already generated
receipt_df = pd.read_pickle('sample\\receipt_df.pkl')

# clean 'small' column from receipt_df
receipt_df2 = receipt_df[['receipt_id','small']].drop_duplicates()
receipt_df2['small'] = receipt_df2['small'].str.replace('<','')
receipt_df2 = receipt_df2[receipt_df2['small'].notna()]

basket = process_data(data = receipt_df2)

result_df = asso_rule(basket_data = basket, 
                      min_sup = 0.01, 
                      min_conf = 0.2)