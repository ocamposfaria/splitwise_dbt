#!/usr/bin/env python
# coding: utf-8

# ### Imports, mySQL connection and API tokens

from splitwise import Splitwise
import pandas as pd
import mysql.connector
from mysql.connector import Error
import yaml



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


with open(r'C:\Users\ocamp\Desktop\splitwise_dbt\splitwise\analyses\secrets\keys.yaml', 'r') as file:
    keys = yaml.safe_load(file)


s = Splitwise(consumer_key=keys['consumer_key'],
              consumer_secret=keys['consumer_secret'],
              api_key=keys['api_key'])

u = s.getCurrentUser()


# ### Get Splitwise expenses

# uncomment to access groups' IDs
df = []
for group in s.getGroups():
    df_d = {
        'group': group.getName(),
        'group_id': group.getId()
    }
    df.append(df_d)
df = pd.DataFrame(df)
print(df)


def get_splitwise_expenses(group_id, group_name, limit):
    expenses = s.getExpenses(limit=limit, group_id=group_id)
    expenses.reverse()
    df = []
    for expense in expenses:
        df_d = {
            "expense_id": expense.getId(),
            "group_id": expense.getGroupId(),
            "name": expense.getDescription(),
            #"getRepeatInterval": expense.getRepeatInterval(),
            #"getEmailReminder": expense.getEmailReminder(),
            #"getEmailReminderInAdvance": expense.getEmailReminderInAdvance(),
            #"getNextRepeat": expense.getNextRepeat(),
            "details": expense.getDetails(),
            #"getCommentsCount": expense.getCommentsCount(),
            #"payment": expense.getPayment(),
            #"creation_method": expense.getCreationMethod(),
            #"transaction_method": expense.getTransactionMethod(),
            #"transaction_confirmed": expense.getTransactionConfirmed(),
            "cost": expense.getCost(),
            #"currency_code": expense.getCurrencyCode(),
            "created_by": expense.getCreatedBy() if expense.getCreatedBy() is None else expense.getCreatedBy().getFirstName(),
            "date": expense.getDate(),
            "created_at": expense.getCreatedAt(),
            "updated_at": expense.getUpdatedAt(),
            "deleted_at": expense.getDeletedAt(),
            #"receipt_original": expense.getReceipt().getOriginal(),
            #"receipt_large": expense.getReceipt().getLarge(),
            #"category_name": expense.getCategory().getName(),
            "updated_by": expense.getUpdatedBy() if expense.getUpdatedBy() is None else expense.getUpdatedBy().getFirstName(),
            "deleted_by": expense.getDeletedBy() if expense.getDeletedBy() is None else expense.getDeletedBy().getFirstName(),
            #"users": [expense.getUsers()[i].getFirstName() for i in range(len(expense.getUsers()))],
            #"expense_bundle_id": expense.getExpenseBundleId(),
            #"friendship_id": expense.getFriendshipId(),
            "repayments": 0 if expense.getRepayments() == [] else expense.getRepayments()[0].getAmount(),
            "repayments_from": "None" if expense.getRepayments() == [] else expense.getRepayments()[0].getFromUser(),
            "repayments_to": "None" if expense.getRepayments() == [] else expense.getRepayments()[0].getToUser(),
            #"receipt_path": expense.getReceiptPath(),
            #"transaction_id": expense.getTransactionId(),
            "source": group_name
        }
        df.append(df_d)

    df = pd.DataFrame(df)
    df = df.replace(27512092, 'João')
    df = df.replace(20401164, 'Hallana')
    df['category'] = df['name'].str.split(']', expand = True)[0]
    df['created_at'] = df['created_at'].str.split('T', expand = True)[0]
    df['updated_at'] = df['updated_at'].str.split('T', expand = True)[0]
    df['date'] = df['date'].str.split('T', expand = True)[0]
    df['deleted_at'] = df['deleted_at'].str.split('T', expand = True)[0]
    try:
        df['name'] = df['name'].str.split(']', expand = True)[1]
        df['category'] = df['category'].replace('\[', '', regex=True)
    except:
        df['category'] = df['name']
        df['name'] = None   
    return df


df_1 = get_splitwise_expenses(33823062, 'Nossa Residência', 2000)
# df_2 = get_splitwise_expenses(34137144, 'VR', 100)
df_3 = get_splitwise_expenses(35336773, 'just me', 100)
df_4 = get_splitwise_expenses(40055224, 'apenas lana', 100)
# df_5 = get_splitwise_expenses(62599381, 'viagem chapada 2024', 200)
# df_5 = get_splitwise_expenses(57014599, 'viagem chapada 2023', 100)
# df_4 = get_splitwise_expenses(37823696, 'Harry Styles', 15)
# df_4 = get_splitwise_expenses(40055224, 'apenas lana', 10)
# df_5 = get_splitwise_expenses(32626795, 'Primavera Sound SP')
# df_5 = get_splitwise_expenses(40780239, 'Black Mobly', 10)
# df_5 = get_splitwise_expenses(39698610, 'Lollapalooza 2023', 15)


df = pd.concat([df_1, df_3, df_4], ignore_index=True)

# ### Getting month

with open(r'C:\Users\ocamp\Desktop\splitwise_dbt\splitwise\analyses\month.txt') as f:
    lines = f.readlines()
month = lines[0].replace('\n', '')


# update

try:
    cursor = mydb.cursor()

    for i in range(df.shape[0]):
        query = f"""
                INSERT INTO splitwise (
                expense_id,
                group_id,
                `name`,
                cost,
                created_by,
               `date`,
                created_at,
                updated_at,
                deleted_at,
                updated_by,
                deleted_by,
                repayments,
                repayments_from,
                repayments_to,
                category,
                month,
                source,
                details)

                VALUES (
                '{df['expense_id'][i]}',
                '{df['group_id'][i]}',
                '{df['name'][i]}',
                '{df['cost'][i]}',
                '{df['created_by'][i]}',
                '{df['date'][i]}',
                '{df['created_at'][i]}',
                '{df['updated_at'][i]}',
                '{df['deleted_at'][i]}',
                '{df['updated_by'][i]}',
                '{df['deleted_by'][i]}',
                '{df['repayments'][i]}',
                '{df['repayments_from'][i]}',
                '{df['repayments_to'][i]}',
                '{df['category'][i]}',
                '{month}',
                '{df['source'][i]}',
                '{df['details'][i]}')

                ON DUPLICATE KEY UPDATE
                expense_id = '{df['expense_id'][i]}',
                group_id = '{df['group_id'][i]}', 
                `name` = '{df['name'][i]}', 
                cost = '{df['cost'][i]}', 
                created_by = '{df['created_by'][i]}', 
                `date` = '{df['date'][i]}', 
                created_at = '{df['created_at'][i]}',
                updated_at = '{df['updated_at'][i]}',
                deleted_at = '{df['deleted_at'][i]}',
                updated_by = '{df['updated_by'][i]}',
                deleted_by = '{df['deleted_by'][i]}',
                repayments = '{df['repayments'][i]}',
                repayments_from = '{df['repayments_from'][i]}',
                repayments_to = '{df['repayments_to'][i]}',
                category = '{df['category'][i]}',
                source = '{df['source'][i]}',
                details = '{df['details'][i]}'     
                """
        # não dou update nos campos 'month' para preservar o mês que ele foi criado
        
        cursor.execute(query)

    mydb.commit()
    cursor.close()
except Exception as e:
    print(query)
    print(e)
    cursor.close()