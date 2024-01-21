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
    list_of_expenses = ['2316635561', '2388942936', '2388785634', '2708129939', '2816905638', '2816901332', '2816907327', '2816900062', '2816689204', '2816902854', '2816676094', '2816677544', '2816676708', '2841153993', '2841137328', '2841148027', '2841165942', '2841166302', '2841153072', '2841151290', '2841149207', '2841146419', '2841144760', '2842663502', '2842669656', '2842664270', '2842709802', '2842712362', '2842743369', '2842670147', '2842658910', '2842668934', '2842662313', '2842669074', '2842654664', '2842660128', '2854886541', '2854870838', '2854889500', '2854869345', '2854888140', '2854903212', '2854884119', '2854903037', '2854903462', '2854870359', '2854868015', '2854863766', '2854868889', '2854855352', '2854893174', '2854891279', '2854863256', '2854890854', '2854887654'], 
    percentage_juau = 0.640986, 
    percentage_lana = 0.359014,
    ano_mes = '2023-12', 
    delete = True)