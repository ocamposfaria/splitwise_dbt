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
        raise Exception(f'A soma dos percentuais tem de ser igual a 1, mas recebi {percentage_juau + percentage_lana}.')

    for i in range(len(list_of_expenses)):
        print('\n \n # # # # # # # # # # # # # # # # # # # # \n \n') 

        try:
            expense_old = s.getExpense(list_of_expenses[i])    
            # print(expense_old)
            repayment_value = expense_old.repayments[0].getAmount()
            # print(repayment_value)
            repayment_from = expense_old.repayments[0].getFromUser()
            # print(repayment_from)

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
            # print('gasto antigo:')
            # pprint(vars(expense_old))
            print('gasto novo:')
            pprint(vars(expense_new))
            print('detalhes de pagamento:')
            pprint(vars(expense_new.users[0]))
            pprint(vars(expense_new.users[1]))
            
            expense_id, error = s.createExpense(expense_new)
            if error:
                pprint(vars(error))
                raise Exception('Splitwise error')
            
            if delete:
                success, errors = s.deleteExpense(list_of_expenses[i])
                if success:
                    print(f'({i}) ID {list_of_expenses[i]} deletado')
                if errors:
                    raise Exception(errors)

        except Exception as e:
            print(f' falha no ID: {list_of_expenses[i]}')
            raise Exception(e)


recreate_expenses_with_new_percentage(
    list_of_expenses =  ['3065788421', '3065788398', '3065748164', '3065745689', '3065744967', '3065743887', '3065743503', '3065743142', '3065730699', '3065729369', '3065728816', '3065727320', '3065726802', '3065725842', '3065724185', '3065705287', '3065704872', '3065704461', '3065702925', '3065701854', '3065692465', '3065690361', '3065689549', '3065688867', '3065687536', '3065687084', '3065684081', '3065678065', '3065672499', '3065672023', '3036232662', '3036228022', '3036187257', '3036156084', '3036121615', '2965420183', '2965419313', '2965408854', '2897192899'], 
    percentage_juau = 0.6379, 
    percentage_lana = 0.3621,
    ano_mes = '2024-04', 
    delete = True)