#!/usr/bin/env python
# coding: utf-8

# In[1]:


from splitwise import Splitwise
from splitwise.expense import Expense, ExpenseUser
import pandas as pd
from pprint import pprint
import yaml


# In[2]:


with open('keys.yaml', 'r') as file:
    keys = yaml.safe_load(file)


# In[3]:


s = Splitwise(consumer_key=keys['consumer_key'],
              consumer_secret=keys['consumer_secret'],
              api_key=keys['api_key'])

u = s.getCurrentUser()


# In[4]:


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


# In[6]:


recreate_expenses_with_new_percentage(
    list_of_expenses = [2193180534, 2316633994, 2374542938, 2388775313, 2388938649, 2388783775, 2390250935, 2420230197, 2420225057, 2420279846, 2420268659, 2420234090, 2420230912, 2420274320, 2420223901, 2420278625, 2420244906, 2423706548, 2423707314, 2423690668, 2423696754, 2423706753, 2423695738, 2423693611, 2423700341, 2423700606, 2423691268, 2423695378, 2423700119, 2423703200, 2423704867, 2423703716, 2423691723, 2423703454, 2423691980, 2423692282, 2423693994, 2423696060, 2439475988, 2439473888, 2439470769, 2439471618, 2439466555, 2439470316, 2439468213, 2439474441, 2441671456, 2441664339, 2441663375, 2441663783, 2441671055, 2441671982, 2465284281, 2465289869, 2465383864, 2465377245, 2465272943, 2465283053, 2465287404, 2465302210, 2465288240, 2465289030, 2465303848, 2465303344, 2465311463], 
    percentage_juau = 0.5463, 
    percentage_lana = 0.4537, 
    ano_mes = '2023-06', 
    delete = True)

