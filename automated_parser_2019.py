import os
import pandas as pd
import subprocess
import requests
from io import StringIO

table_name = lambda param: '_'.join(['t', param[0], param[1], param[2]])


# TODO: Test each year and season
def parse_func(param, connection):
    ''' 
        Parse data of a season and store it into sqlite table.
        Required dependency data will be parsed if absent.
        Data dependency: Q4 -> Q3 -> Q2 -> Q1

        param (str, str, str): (stock id, year, season) 
        connection: sqlite connection

        Note: If you call this function with the same parameters again, the original data will be replaced by new data.

        Note: 損益表
        1. for Q4, we will get the yearly data, so we should subtract it from Q1 + Q2 + Q3 data

        Note: 現金流量表
        1. for Q2, Q3 & Q4, we will get the accumulated data, so we should subtract it from the data of previous season
    '''

    cursor = connection.cursor()
    table = table_name(param)
    

    ####### dependency check #######
    table_list = set()
    ls_table_query = "SELECT name FROM sqlite_schema WHERE type ='table' AND name NOT LIKE 'sqlite_%';"
    for row in cursor.execute(ls_table_query):  table_list.add(row[0])

    # Q4 dependency: Q1, Q2, Q3
    # check order: Q1 -> Q2 -> Q3
    if param[2] == '4':
        for season in ['1', '2', '3']:
            new_param = param[0], param[1], season
            if table_name(new_param) not in table_list:
                print(f'\nMissing required dependency: Q{season}')
                print(f'Start parsing dependencies...')
                parse_func(new_param, connection)
    
    # Q2, Q3 dependency: previous season
    elif param[2] in ['2', '3']:
        new_param = param[0], param[1], str(int(param[2])-1)
        if table_name(new_param) not in table_list:
            print(f'\nMissing required dependency: Q{new_param[2]}')
            print(f'Start parsing dependencies...')
            parse_func(new_param, connection)
        
    print('----------------------------------------------------------')
    
    ####### process target #######
    # transaction occur
    try:
        # create current table (sqlite)
        query = 'CREATE TABLE IF NOT EXISTS ' + table + ' (\
                    Code TEXT PRIMARY KEY, \
                    Title TEXT, \
                    Money BIGINT \
                    );'
        print('[QUERY] ' + query) 
        cursor.execute(query)

        dfs = parse_func_helper(param, connection)

        ####### Post-process target #######
        # TODO: parallel processing

        # process Q4 損益表
        if param[2] == '4':
            income_Code = dfs[1]['Code'].to_list()
            process_income(param, connection, income_Code)

        # process Q2 or Q3 or Q4 現金流量表
        if param[2] in ['2', '3', '4']:
            cash_Code = dfs[2]['Code'].to_list()
            process_cash(param, connection, cash_Code)

    # if any error occur, drop corrupted table
    except:
        query = f"DROP TABLE {table};"
        print('[QUERY] ' + query) 
        cursor.execute(query)
        connection.commit()
        print(f'Fail to parse table: {table}')
        print('----------------------------------------------------------')
        raise
    
    # if everything is successful, commit all changes to current table at once
    connection.commit()
    print(f'Complete table: {table}')
    print('----------------------------------------------------------')


def parse_func_helper(param, connection):
    ''' 
        parsing core function
    '''
    cursor = connection.cursor()
    table = table_name(param)
    url = f'https://mops.twse.com.tw/server-java/t164sb01?step=1&CO_ID={param[0]}&SYEAR={param[1]}&SSEASON={param[2]}&REPORT_ID=C'


    # clear data (sqlite)
    query = f"DELETE FROM {table};"
    print('[QUERY] ' + query)
    cursor.execute(query)

    # parsing
    print(f'parsing: {url}\n')
    res = requests.get(url)
    res.encoding = 'big5'

    try:
        dfs = pd.read_html(StringIO(res.text))
    except ValueError:
        print('Error: data not available...')
        raise

    # dfs[0]    資產負債表
    # dfs[1]    損益表
    # dfs[2]    現金流量表

    # save three financial statements
    for i in range(3):
        # drop useless columns
        dfs[i] = dfs[i].iloc[:, 0:3]
        dfs[i].columns = ['Code', 'Title', 'Money']

        # drop useless rows (that has blank in "Code")
        dfs[i].dropna(subset='Code', inplace = True)

        # all columns into string (for ease of regex processing)
        dfs[i] = dfs[i].astype('string')

        # eliminate ".0" induced by float type
        dfs[i]['Code'] = dfs[i]['Code'].str.replace('\.0','', regex=True)
        
        # eliminate parenthesis and commas
        for pattern in ['(', ')', ',']:
            dfs[i]['Money'] = dfs[i]['Money'].str.replace(pattern,'', regex=True)
        
        # convert "Money" to numeric
        dfs[i]['Money'] = pd.to_numeric(dfs[i]['Money'])
        
        # load data into sqlite
        data = dfs[i].values.tolist()
        query = "INSERT INTO " + table + " VALUES (?, ?, ?);"
        cursor.executemany(query, data)

        # dfs[i].to_csv(path, encoding='utf-8', index=False)

    return dfs


def process_income(param, connection, income_Code):
    '''
        Q4 - (Q1+Q2+Q3)

        Note: if the previous record does not have matching element, do nothing. 
            I suppose all important rows we need will have their counterparts in the previous season.
            For some rows that does not appear in every year, they might not be important, so we do nothing.  
    '''
    cursor = connection.cursor()
    table = table_name(param)
    prev_param = [param[0], param[1], None]

    # TODO: parallel processing
    for Code in income_Code:
        cur_q = f"SELECT Money FROM {table} WHERE Code = '{Code}'"
        tmp = cur_val = cursor.execute(cur_q).fetchone()[0]

        for season in ['1', '2', '3']:
            prev_param[2] = season
            prev_table = table_name(prev_param)

            prev_q = f"SELECT Money FROM {prev_table} WHERE Code = '{Code}'"
            prev_val = cursor.execute(prev_q).fetchone()

            if prev_val is not None:
                cur_val -= prev_val[0]
    
        if cur_val != tmp:
            query = f"UPDATE {table} SET Money={cur_val} WHERE Code = '{Code}'"
            cursor.execute(query)


def process_cash(param, connection, cash_Code):
    '''
        cur season - prev season
        
        Note: if the previous record does not have matching element, do nothing. 
            I suppose all important rows we need will have their counterparts in the previous season.
            For some rows that does not appear in every year, they might not be important, so we do nothing.    
    '''
    cursor = connection.cursor()
    table = table_name(param)
    prev_param = param[0], param[1], str(int(param[2])-1)
    prev_table = table_name(prev_param)

    # TODO: parallel processing
    for Code in cash_Code:
        prev_q = f"SELECT Money FROM {prev_table} WHERE Code = '{Code}'"
        cur_q = f"SELECT Money FROM {table} WHERE Code = '{Code}'"

        prev_val = cursor.execute(prev_q).fetchone()
        cur_val = cursor.execute(cur_q).fetchone()[0]

        if prev_val is not None:
            cur_val -= prev_val[0]
            query = f"UPDATE {table} SET Money={cur_val} WHERE Code = '{Code}'"
            cursor.execute(query)

