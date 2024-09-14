# -*- coding: utf-8 -*-
"""
Created on Fri Sep 13 12:02:35 2024

@author: elisa
"""

import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

class Query_Data_From_Postgresql():
    
    def __init__(self, dbname, user, pw, host, port):
        # init with db info
        self.dbname = dbname
        self.user = user
        self.pw = pw
        self.host = host
        self.port = port
    
    
    def query_data_from_pg(self, sql_str):
        """
        sql_str: 需要查询的sql
        return: DataFrame
        """
        
        # build DB connection
        conn = psycopg2.connect(database = self.dbname, 
                                user = self.user, 
                                password = self.pw,
                                host = self.host,
                                port = self.port)
        cur = conn.cursor()
        sql = sql_str
        cur.execute(sql)
        
        # query data and convert to dataframe
        df = pd.DataFrame(cur.fetchall()) 
        
        # get column names
        colnames = [t[0] for t in cur.description]
        df.columns = colnames
        cur.close()
        conn.close()
        
        return(df)


if __name__ == '__main__':
    
    # NOTE: this .env file is not uploaded because it contains sensitive information
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
    
    sql_str = 'select * from receipts limit 5'
    df = query_data.query_data_from_pg(sql_str)
    
    
    