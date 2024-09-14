# -*- coding: utf-8 -*-
"""
Created on Fri Sep 13 12:34:20 2024

@author: elisa
"""

from association_rules.Query_Data_From_Postgresql import Query_Data_From_Postgresql
from association_rules.sql_text import *

import numpy as np
import pandas as pd
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules


# process data and convert it to basket format
def process_data(data):
    '''
    data: sales data or activity data
    return: basket data，list
    
    '''
    # remove orders with only one item
    receipt_cn = pd.value_counts(data.receipt_id)
    receipt_cn_use = receipt_cn.loc[receipt_cn > 1]

    # generate basket data based on unique receipt_id
    receipt_id_uni = receipt_cn_use.index
    receipt_num = len(receipt_id_uni)
    
    colnames = data.columns
    basket = [] * receipt_num
    for i in range(receipt_num):
        df_temp = data.loc[data.receipt_id == receipt_id_uni[i],:]
        list_temp = list(set(df_temp[colnames[-1]]))
        basket.append(list_temp)
        
    return(basket)


# association rules
def asso_rule(basket_data, min_sup, min_conf):
    '''
    basket_data : basket data
    min_sup : minimum support
    min_conf : minimum confidence
    Returns： dataframe with association rules results

    '''

    # transform basket data into required format for apriori algorithm
    te = TransactionEncoder()
    df_tf = te.fit_transform(basket_data)
    df = pd.DataFrame(df_tf, columns = te.columns_)
    
    # calculate frequent item sets
    freq_itemsets = apriori(df, min_support = min_sup, use_colnames = True)
    freq_itemsets.sort_values(by = 'support', ascending = False, inplace = True)

    # calculate association rules
    df_result = association_rules(freq_itemsets, 
                                  metric = 'confidence', 
                                  min_threshold = min_conf)
     
    # sort association rules by lift
    df_result.sort_values(by='lift',ascending=False,inplace=True)    
    
    # rule： antecedents->consequents
    return(df_result)


if __name__ == '__main__':
    
    load_dotenv('pg_conn.env')
    
    DBNAME = os.getenv('DBNAME')
    USER = os.getenv('USER')
    PW = os.getenv('PW')
    HOST = os.getenv('HOST')
    PORT = os.getenv('PORT')
    
    query_data = Query_Data_From_Postgresql(dbname = DBNAME,
                                            user = USER,
                                            pw = PW,
                                            host = HOST,
                                            port = PORT)
    
    receipt_df = query_data.query_data_from_pg(receipt_sql.format('2022-08-01','2022-08-02'))
    
    receipt_df2 = receipt_df[['receipt_id','small']].drop_duplicates()
    receipt_df2['small'] = receipt_df2['small'].str.replace('<','')
    receipt_df2 = receipt_df2[receipt_df2['small'].notna()]
    
    basket = process_data(data = receipt_df2)
    
    result_df = asso_rule(basket_data = basket, 
                          min_sup = 0.01, 
                          min_conf = 0.2)




