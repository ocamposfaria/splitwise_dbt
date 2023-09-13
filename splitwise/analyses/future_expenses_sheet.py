#!/usr/bin/env python
# coding: utf-8

import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

# Configurar as credenciais da conta de serviço
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(r'C:\Users\ocamp\Desktop\splitwise_dbt\splitwise\analyses\secrets\sinuous-concept-307423-38657574a1a5.json', scope)

# Autenticar e abrir a planilha
client = gspread.authorize(credentials)
planilha = client.open('Gastos futuros')

# Selecionar a primeira guia da planilha
guia = planilha.get_worksheet(0)

# Obter todos os valores da guia
dados = guia.get_all_values()

# Criar o DataFrame pandas
df = pd.DataFrame(dados[1:], columns=dados[0])


df = df.apply(lambda x: x.str.replace(',','.'))
df


import mysql.connector
from mysql.connector import Error


# old aws
# mydb = mysql.connector.connect(host= 'accountingbob.cs8refzom5ab.us-east-1.rds.amazonaws.com',
#                                      database='bob',
#                                      user='admin',
# password='oldnumber7')

# new azure
# mydb = mysql.connector.connect(host= 'nossa-residencia-mysql-azure-server.mysql.database.azure.com',
#                                      database='bob',
#                                      user='ocamposfaria',
#                                      password='oldnumber7.Tennessee')

# localhost
mydb = mysql.connector.connect(host= '127.0.0.1',
                                     database='bob',
                                     user='ocamp',
                                     password='oldnumber7.Tennessee')

if mydb.is_connected():
    db_Info = mydb.get_server_info()
    print("Connected to MySQL Server version ", db_Info)
else:
    print("Error, not conected")


try:
    cursor = mydb.cursor()

    query = f"""
            TRUNCATE TABLE bob.future_expenses_sheet         
            """
        
    cursor.execute(query)

    mydb.commit()
    cursor.close()
except Exception as e:
    print(query)
    print(e)
    cursor.close()


try:
    cursor = mydb.cursor()

    for i in range(df.shape[0]):
        query = f"""
                INSERT INTO future_expenses_sheet (
                `month`,
                `group`,
                `category`,
                `cost_juau`,
                `cost_lana`
                )

                VALUES (
                '{df['month'][i]}',
                '{df['group'][i]}',
                '{df['category'][i]}',
                '{df['cost_juau'][i]}',
                '{df['cost_lana'][i]}'
                )                
                """
        
        cursor.execute(query)

    mydb.commit()
    cursor.close()
except Exception as e:
    print(query)
    print(e)
    cursor.close()