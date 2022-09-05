import os
import pandas as pd
import subprocess
import requests
from io import StringIO

table_name = lambda param: '_'.join(['t', param[0], param[1], param[2]])


def parse_func(param, connection):
    ''' 
        Parse data of a season and store it into sqlite table.
        Required dependency data will be parsed if absent.
        Data dependency: Q4 -> Q3 -> Q2 -> Q1

        param (str, str, str): (stock id, year, season) 
        connection: sqlite connection

        Note: If you call this function with the same parameters again, the original data will be replaced by new data.

        Note: 損益表
        1. for Q4, we will get the accumulated data, so we should subtract it from Q1 + Q2 + Q3 data

        Note: 現金流量表
        1. for Q2, Q3 & Q4, we will get the accumulated data, so we should subtract it from the data of all previous seasons
    '''

    cursor = connection.cursor()
    table = table_name(param)
    

    ####### dependency check #######
    table_list = set()
    ls_table_query = "SELECT name FROM sqlite_schema WHERE type ='table' AND name NOT LIKE 'sqlite_%';"
    for row in cursor.execute(ls_table_query):  table_list.add(row[0])

    # dependency: all previous seasons in the same year
    # ex: Q4 dependency: Q1, Q2, Q3
    # check order: Q1 -> Q2 -> Q3
    if param[2] in ['2', '3', '4']:
        L = int(param[2])
        for season in range(1, L):
            new_param = param[0], param[1], str(season)
            if table_name(new_param) not in table_list:
                print(f'\nMissing required dependency: Q{season}')
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

        # process Q4 損益表
        if param[2] == '4':
            income_Code = dfs[1]['Code'].to_list()
            process_accumulated_data(param, connection, income_Code)

        # process Q2 or Q3 or Q4 現金流量表
        if param[2] in ['2', '3', '4']:
            cash_Code = dfs[2]['Code'].to_list()
            process_accumulated_data(param, connection, cash_Code)

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
        parsing core function (2019 ~)

        1. insert parsed data (sqlite)
        2. return pandas table
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
        dfs = pd.read_html(StringIO(res.text))[0:3]
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
        
        # eliminate parenthesis(to minus sign) and commas
        pat_repl_dict = {'(': '-', ')':'', ',':''}
        for pat, repl in pat_repl_dict.items():
            dfs[i]['Money'] = dfs[i]['Money'].str.replace(pat, repl, regex=True)
        
        # convert "Money" to numeric
        dfs[i]['Money'] = pd.to_numeric(dfs[i]['Money'])
        
        # load data into sqlite
        data = dfs[i].values.tolist()
        query = "INSERT INTO " + table + " VALUES (?, ?, ?);"
        cursor.executemany(query, data)

        # dfs[i].to_csv(path, encoding='utf-8', index=False)

    return dfs


def process_accumulated_data(param, connection, Codes):
    '''
        Given codes that have accumulated data, calculate their current data
        ex: Q4 - (Q1+Q2+Q3)

        Note: if the previous record does not have matching element, do nothing. 
            I suppose all important rows we need will have their counterparts in the previous season.
            For some rows that does not appear in every year, they might not be important, so we do nothing.  
    '''
    cursor = connection.cursor()
    table = table_name(param)
    prev_param = [param[0], param[1], None]
    L = int(param[2])

    for code in Codes:
        cur_q = f"SELECT Money FROM {table} WHERE Code = '{code}'"
        tmp = cur_val = cursor.execute(cur_q).fetchone()[0]

        for season in range(1, L):
            prev_param[2] = str(season)
            prev_table = table_name(prev_param)

            prev_q = f"SELECT Money FROM {prev_table} WHERE Code = '{code}'"
            prev_val = cursor.execute(prev_q).fetchone()

            if prev_val is not None:
                cur_val -= prev_val[0]
    
        if cur_val != tmp:
            query = f"UPDATE {table} SET Money={cur_val} WHERE Code = '{code}'"
            cursor.execute(query)
