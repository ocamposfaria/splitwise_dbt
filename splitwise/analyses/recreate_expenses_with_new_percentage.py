#!/usr/bin/env python
# coding: utf-8

from splitwise import Splitwise
from splitwise.expense import Expense, ExpenseUser
import pandas as pd
from pprint import pprint
import yaml


with open(r'C:\Users\ocamp\Desktop\splitwise_dbt\splitwise\analyses\secrets\keys.yaml', 'r') as file:
    keys = yaml.safe_load(file)


s = Splitwise(consumer_key=keys['consumer_key'],
              consumer_secret=keys['consumer_secret'],
              api_key=keys['api_key'])

u = s.getCurrentUser()


def recreate_expenses_with_new_percentage(list_of_expenses, percentage_juau, percentage_lana, ano_mes, delete):
    
    if percentage_juau + percentage_lana != 1:
        raise Exception('A soma dos percentuais tem de ser igual a 1.')

    for i in range(len(list_of_expenses)):
        try:
            expense_old = s.getExpense(list_of_expenses[i])    
            repayment_value = expense_old.repayments[0].getAmount()
            repayment_from = expense_old.repayments[0].getFromUser()

            if repayment_from == 20401164: # juau pagou, então o repayment é from lana to juau
                juau = ExpenseUser()
                juau.setId(27512092)
                juau.setPaidShare(expense_old.cost)
                juau.setOwedShare(str(round(float(expense_old.cost) * percentage_juau, 2)))

                lana = ExpenseUser()
                lana.setId(20401164)
                lana.setPaidShare('0.00')
                lana.setOwedShare(str(round(float(expense_old.cost) * percentage_lana, 2)))

            if repayment_from == 27512092: # lana pagou, então o repayment é from juau to lana
                lana = ExpenseUser()
                lana.setId(20401164) 
                lana.setPaidShare(expense_old.cost)
                lana.setOwedShare(str(round(float(expense_old.cost) * percentage_lana, 2)))

                juau = ExpenseUser()
                juau.setId(27512092)
                juau.setPaidShare('0.00')
                juau.setOwedShare(str(round(float(expense_old.cost) * percentage_juau, 2)))

            expense_new = Expense()
            expense_new.setCost(expense_old.cost)
            expense_new.addUser(juau)
            expense_new.addUser(lana)
            expense_new.setGroupId(33823062) # Nossa Residência
            expense_new.setDescription(expense_old.description)
            # expense_new.setDate(expense_old.date)
            expense_new.setDetails(ano_mes + '\n recreated with script to update past percentages')
            
            # pprint(vars(expense_new))
            # pprint(vars(expense_new.users[0]))
            # pprint(vars(expense_new.users[1]))
            
            expense_id, error = s.createExpense(expense_new)
            if error:
                pprint(vars(error))
                raise Exception('Splitwise error')
            
            if delete:
                s.deleteExpense(list_of_expenses[i])
        except Exception as e:
            print(f' falha no ID: {list_of_expenses[i]}')
            raise Exception(e)


recreate_expenses_with_new_percentage(
    list_of_expenses = [], 
    percentage_juau = 0.5463, 
    percentage_lana = 0.4537, 
    ano_mes = '2023-08', 
    delete = True)