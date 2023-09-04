#!/usr/bin/env python
# coding: utf-8

# ## Imports


import pandas as pd
from splitwise import Splitwise
# import sqlalchemy as sqla
import mysql.connector
from mysql.connector import Error


# ## User inputs


# ---- 2023-03 ----
# salario_lana = 2721
# VR_lana = 803 # 774
# salario_juau = 2987.36
# VR_juau = 500
# PA_juau = 0

# ---- 2023-04 ----
# salario_lana = 2721
# VR_lana = 565
# salario_juau = 2987.36
# VR_juau = 500
# PA_juau = 0

# ---- 2023-05 ----
# salario_lana = 3128.83
# VR_lana = 714
# salario_juau = 2987.36
# VR_juau = 500
# PA_juau = 0

# ---- 2023-06 ----
# salario_lana = 2721
# VR_lana = 565
# salario_juau = 2987.36
# VR_juau = 500
# PA_juau = 0

# ---- 2023-07 ----
# definidos no dbt


# ## Percentages


# renda_total_sem_VR = salario_lana + salario_juau + PA_juau



# percentual_juau = (salario_juau + PA_juau)/renda_total_sem_VR
# percentual_lana = (salario_lana)/renda_total_sem_VR

# mudar padrão no app do splitwise (não funciona no navegador)



percentual_juau =  0.5377202119059684
percentual_lana = 0.4622797880940316

VR_juau = 0
VR_lana = 0



print(percentual_juau*100)



print(percentual_lana*100)


# ## Categories


# df = {
#     'category': ['contas',          'mercado', 'feira', 'padaria', 'alimentação', 'assinaturas', 'outros', 'transporte', 'apenas joão', 'apenas lana'],
#     'cost':     [    2315,  VR_juau + VR_lana,     400,       100,           600,           150,      100,          250,               0,             0]
# }



df = {
    'category': ['contas',          'mercado', 'conveniência', 'alimentação', 'assinaturas', 'outros', 'transporte', 'apenas joão', 'apenas lana'],
    'cost':     [    2300,               1400,            240,           500,           110,       250,         350,                 120,             0]
}



df = pd.DataFrame(df)



df



def cost_juau(row, percentual_juau, VR_juau, VR_lana):
    if row['category'] == 'mercado':
        return VR_juau + ((row['cost'] - VR_lana - VR_juau))*percentual_juau
    elif row['category'] == 'apenas joão':
        return row['cost']
    elif row['category'] == 'apenas lana':
        return 0
    else:
        return row['cost']*percentual_juau

df['cost_juau'] = df.apply(cost_juau, percentual_juau=percentual_juau, VR_juau=VR_juau, VR_lana=VR_lana, axis=1)



def cost_lana(row, percentual_lana, VR_juau, VR_lana):
    if row['category'] == 'mercado':
        return VR_lana + ((row['cost'] - VR_lana - VR_juau))*percentual_lana
    elif row['category'] == 'apenas lana':
        return row['cost']
    elif row['category'] == 'apenas joão':
        return 0
    else:
        return row['cost']*percentual_lana

df['cost_lana'] = df.apply(cost_lana, percentual_lana=percentual_lana, VR_juau=VR_juau, VR_lana=VR_lana, axis=1)


# ## Sending Data


# old aws
# mydb = mysql.connector.connect(host= 'accountingbob.cs8refzom5ab.us-east-1.rds.amazonaws.com',
#                                      database='bob',
#                                      user='admin',
# password='oldnumber7')

# old azure
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



with open('month.txt') as f:
    lines = f.readlines()
month = lines[0].replace('\n', '')
df['month'] = month
df



for i in range(df.shape[0]):
    query = f'''
    INSERT INTO

    limits
    (category,
    cost,
    cost_juau,
    cost_lana,
    month)

    VALUES(
    '{df['category'][i]}',
    '{df['cost'][i]}',
    '{df['cost_juau'][i]}',
    '{df['cost_lana'][i]}',
    '{df['month'][i]}'
    )
    
    ON DUPLICATE KEY UPDATE
    
    category = '{df['category'][i]}',
    cost = '{df['cost'][i]}',
    cost_juau = '{df['cost_juau'][i]}',
    cost_lana = '{df['cost_lana'][i]}',
    month = '{df['month'][i]}'

    '''

    try:
        cursor = mydb.cursor()
        cursor.execute(query)
        mydb.commit()
        cursor.close()
    except Exception as e:
        print(query)
        print(e)