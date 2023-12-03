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
    list_of_expenses = ['2316635349', '2388941968', '2388785399', '2708129704', '2736858430', '2765052087', '2765299075', '2764856938', '2765088764', '2765300857', '2764871538', '2765076124', '2765284694', '2764876936', '2765301568', '2777874777', '2778010593', '2777869677', '2778040706', '2778014977', '2777996091', '2778030120', '2778022333', '2777883565', '2777875221', '2778023867', '2778041775', '2777891504', '2777871819', '2778025064', '2778024586', '2789556768', '2789546164', '2789883343', '2789545267', '2789547629', '2789548094', '2789544310', '2789545819', '2789557400', '2789546875', '2816945922', '2816869811', '2816634823', '2816925654', '2816880761', '2816867022', '2816927341', '2816619566', '2816945188', '2816618481', '2816886863', '2816606145', '2816890407', '2816881408', '2816612673', '2816664684', '2816893567', '2816893937', '2816663541', '2816674821', '2816875412', '2816892770', '2816867899', '2816620075'], 
    percentage_juau = 0.616522, 
    percentage_lana = 0.383478, 
    ano_mes = '2023-11', 
    delete = True)