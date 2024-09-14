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


# 处理相关数据，调整数据格式为购物篮
def process_data(data):
    '''
    data: 订单或浏览数据集
    return: 购物篮数据，list
    
    '''
    # 剔除订单中商品数量为1个的订单
    receipt_cn = pd.value_counts(data.receipt_id)
    receipt_cn_use = receipt_cn.loc[receipt_cn > 1]

    # 基于receipt_id生成购物篮数据
    receipt_id_uni = receipt_cn_use.index
    receipt_num = len(receipt_id_uni)
    
    # 将数据格式整理为购物篮
    colnames = data.columns
    basket = [] * receipt_num
    for i in range(receipt_num):
        df_temp = data.loc[data.receipt_id == receipt_id_uni[i],:]
        list_temp = list(set(df_temp[colnames[-1]]))
        basket.append(list_temp)
        
    # 输出数据集
    return(basket)


# 关联规则
def asso_rule(basket_data, min_sup, min_conf):
    '''
    basket_data : 购物篮数据集
    min_sup : 最小支持度
    min_conf : 最小置信度
    Returns： 关联规则结果数据集

    '''
 
    # 将购物篮数据整理成apriori算法所需的数据格式
    te = TransactionEncoder()
    df_tf = te.fit_transform(basket_data)
    df = pd.DataFrame(df_tf, columns = te.columns_)
    
    # 计算频繁项集
    freq_itemsets = apriori(df, min_support = min_sup, use_colnames = True)
    freq_itemsets.sort_values(by = 'support', ascending = False, inplace = True)

    # 计算关联规则
    df_result = association_rules(freq_itemsets, 
                                  metric = 'confidence', 
                                  min_threshold = min_conf)
     
    #关联规则可以提升度排序
    df_result.sort_values(by='lift',ascending=False,inplace=True)    
    
    # 规则是：antecedents->consequents
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




