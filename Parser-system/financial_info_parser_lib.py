import os
import traceback
import pandas as pd
import subprocess
import requests
from io import StringIO
from CustomException import RequestError

table_name = lambda param: '_'.join(['t', param[0], param[1], param[2]])

logger = open(os.path.join('log', "parsing.txt"), "a")


# [資產負債表, 損益表, 現金流量表]
# map column names to standard codes
Standard_Code_mapping = [
{
    '1170': '1170', 
    '應收帳款淨額': '1170', 
    '13000': '1170', # 金融公司
    '應收款項－淨額': '1170', # 金融公司
    # '應收款項-淨額': '1170', # 金融公司 # before 2013

    # 當作應收帳款 (非常態)
    # '應收票據淨額': '1170',
    # '1150': '1170', 

    '11XX': '11XX', 
    '流動資產合計': '11XX', 
    '流動資產': '11XX',

    '130X': '130X',
    '存貨': '130X',
    '存貨合計': '130X',

    '15XX': '15XX',
    '非流動資產合計': '15XX',

    '1600': '1600',
    '不動產、廠房及設備': '1600',
    '不動產、廠房及設備合計': '1600',
    # '固定資產成本合計': '1600',  # before 2013
    # '固定資產淨額': '1600',  # before 2013

    '1780': '1780', 
    '無形資產': '1780', 
    '無形資產合計': '1780',
    '無形資產-淨額': '1780', # 金融公司
    '無形資產－淨額': '1780', # 金融公司
    '19000': '1780', # 金融公司 

    '1900': '1900',
    '其他非流動資產': '1900',
    '其他非流動資產合計': '1900',
    # '其他資產合計': '1900', # before 2013

    '1XXX': '1XXX', # 一般公司
    '資產總計': '1XXX', 
    '資產總額': '1XXX',
    '19999': '1XXX',  # 金融公司
    '10000': '1XXX',  # 金融公司, (2801.TW)

    '21XX': '21XX', 
    '流動負債合計': '21XX', 
    '流動負債': '21XX',

    '25XX': '25XX', 
    '非流動負債合計': '25XX', 

    '2600': '2600',
    '其他非流動負債': '2600',
    '其他非流動負債合計': '2600', 
    # '其他負債合計': '2600',  # before 2013
    
    # Note: 少數公司有的資訊
    '3120': '3120',
    '特別股股本': '3120',
    
    '3100': '3100', # 一般公司
    '股本合計': '3100', # 一般公司 金融公司
    '31100': '3100', # 金融公司

    '3XXX': '3XXX', # 一般公司
    '權益總計': '3XXX', # 一般公司 金融公司
    '權益總額': '3XXX', # 一般公司 金融公司
    '39999': '3XXX', # 金融公司
    '30000': '3XXX', # 金融公司 (2801.TW)
    '股東權益總計': '3XXX'
},

{
    '4000': '4000', 
    '營業收入合計': '4000', 

    '5000': '5000',
    '營業成本合計': '5000',

    '5900': '5900', 
    '營業毛利（毛損）': '5900', 
    # '營業毛利(毛損)': '5900', # before 2013

    '6900': '6900', 
    '營業利益（損失）': '6900', 
    # '營業淨利(淨損)': '6900', # before 2013

    # 稅前淨利
    # Note: 同時出現在損益表、現金流量表
    '7900': '7900', # 一般公司 金融公司  
    '繼續營業單位稅前淨利（淨損）': '7900', # 一般公司 金融公司 # 出現在損益表、現金流量表 
    '繼續營業單位稅前損益': '7900', # 一般公司 金融公司 # 出現在損益表、現金流量表 
    '64001': '7900', # 金融公司 (2801.TW)
    # '繼續營業單位稅前合併淨利（淨損）': '7900', # 一般公司 金融公司 # 出現在損益表、現金流量表 # before 2013
    # '繼續營業單位稅前淨利(淨損)': '7900', # 一般公司 金融公司 # 出現在損益表、現金流量表 # before 2013

    # 稅後淨利
    '8200': '8200' , # 一般公司 金融公司
    '本期淨利（淨損）': '8200',
    '69000': '8200', # 金融公司
    '64000': '8200', # 金融公司 (2801.TW)
    '本期稅後淨利（淨損）': '8200'
    # '繼續營業單位稅後合併淨利(淨損)': '8200', # before 2013
    # '繼續營業單位淨利(淨損)': '8200', # before 2013

},

{   # 金融公司的現金流量表，代號與名稱與一般公司相同

    # 金融公司 代號 61001 對應的欄位不一致，改用A0010
    'A00010': '7900', # 金融公司

    # '折舊費用': 'A20100', 
    # '攤銷費用': 'A20200', ˋ
    'AAAA': 'AAAA',   # 一般公司 金融公司
    '營業活動之淨現金流入（流出）': 'AAAA',
    # '營業活動之淨現金流入(流出)': 'AAAA', # before 2013
    # '營業活動之淨現金流入': 'AAAA', # before 2013
    
    'B02700': 'B02700', # 一般公司 金融公司
    '取得不動產、廠房及設備': 'B02700', 
    '取得不動產及設備': 'B02700'
    # '購置固定資產': 'B02700', # before 2013
    # '購置資產': 'B02700' # before 2013
}]


def parse_func(param, connection):
    ''' 
        Parse data of a season and store it into sqlite table.

        param (str, str, str): (stock id, year, season) 
        cursor: sqlite cursor
        
        Note: Data dependency: Q4 -> Q3 -> Q2 -> Q1

        Note: If you call this function with the same parameters again, the original data will be replaced by new data.

        Note: company id should NOT be provided with ".TW" suffix

        Note: 損益表
        1. for Q4, we will get the accumulated data, so we should subtract it from Q1 + Q2 + Q3 data

        Note: 現金流量表
        1. for Q2, Q3 & Q4, we will get the accumulated data, so we should subtract it from the data of all previous seasons
    '''

    cursor = connection.cursor()
    table = table_name(param)
    id, Year, Quarter = param

    # ####### dependency check #######
    # table_list = set()
    # ls_table_query = 'SELECT name FROM sqlite_schema WHERE type ="table" AND name NOT LIKE "sqlite_%";'
    # for row in cursor.execute(ls_table_query):  table_list.add(row[0])

    # # dependency: all previous seasons in the same year
    # # ex: Q4 dependency: Q1, Q2, Q3
    # # check order: Q1 -> Q2 -> Q3
    # if Quarter in ['2', '3', '4']:
    #     L = int(Quarter)
    #     for season in range(1, L):
    #         new_param = id, Year, str(season)
    #         if table_name(new_param) not in table_list:
    #             print(f'\nMissing required dependency: Q{season}')
    #             print(f'Start parsing dependencies...')
    #             parse_func(new_param, connection)
        
    print(f'[INFO] 🟢 Start parser')

    try:
        # year 2019 ~
        if int(Year) >= 2019:
            dfs = _parse_func_helper(param, cursor)
        # year 2013 ~ 2018
        elif int(Year) >= 2013:
            dfs = _parse_func_helper_2013_2018(param, cursor)
        else:
            dfs = _parse_func_helper_before_2013(param, cursor)

        ####### extra processing #######
        # process Q4 損益表, 2013之前的損益表為累計資料
        if Quarter == '4' or (int(Year) < 2013 and Quarter in ['2', '3', '4']):
            income_Code = dfs[1]['Code'].to_list()
            _process_accumulated_data(param, cursor, income_Code)

        # process Q2 or Q3 or Q4 現金流量表
        if Quarter in ['2', '3', '4']:
            cash_Code = dfs[2]['Code'].to_list()
            _process_accumulated_data(param, cursor, cash_Code)
    except Exception as e:
        error_message = f'[ERROR] ❌ parsing: {table}\n'
        logger.write(error_message)
        print(error_message)
        traceback.print_exc()
        # connection.rollback()   # not needed
        return

    
    connection.commit()
    print(f'\n[INFO] ✅ Complete parsing: {table}')
    print('----------------------------------------------------------')


def _parse_func_helper(param, cursor):
    ''' 
        parsing core function (2019 ~)

        1. insert parsed data (sqlite)
        2. return pandas table [資產負債表, 損益表, 現金流量表]
    '''
    table = table_name(param)
    id, Year, Quarter = param
    url = f'https://mops.twse.com.tw/server-java/t164sb01?step=1&CO_ID={id}&SYEAR={Year}&SSEASON={Quarter}&REPORT_ID=C'


    # create current table (sqlite)
    query = 'CREATE TABLE IF NOT EXISTS ' + table + ' (\
                Code TEXT, \
                Title TEXT, \
                Money BIGINT \
                );'
    print('[INFO] query: ' + query) 
    cursor.execute(query)

    # clear data (sqlite)
    query = f'DELETE FROM {table};'
    print('[INFO] query: ' + query)
    cursor.execute(query)

    # parsing
    print(f'[INFO] parsing: {url}\n')
    res = requests.get(url)
    res.encoding = 'big5'

    try:
        dfs = pd.read_html(StringIO(res.text))[0:3]
    except ValueError:
        print('[ERROR] data not available..., could be due to too many request')
        raise RequestError

    try:
        # must have three sheets
        for i in range(3):
            # must have three columns
            if len(dfs[i].columns) < 3:
                raise ValueError
    except (ValueError, IndexError):
        print('[ERROR] data not available..., could be due to too many request')
        raise RequestError

    # dfs[0]    資產負債表
    # dfs[1]    損益表
    # dfs[2]    現金流量表

    # clear old csv
    path = os.path.join('..', 'csv', 'financial_info', f"{table}.csv")
    if os.path.exists(path):
        os.remove(path)

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
        
        # eliminate right parenthesis and commas, replace left parenthesis with minus sign (negative number)
        pat_repl_dict = {'(': '-', ')':'', ',':''}
        for pat, repl in pat_repl_dict.items():
            dfs[i]['Money'] = dfs[i]['Money'].str.replace(pat, repl, regex=True)
        
        # convert "Money" to numeric
        dfs[i]['Money'] = pd.to_numeric(dfs[i]['Money'])

        # convert to standardized Code（financial companies 金融公司)
        for key, val in Standard_Code_mapping[i].items():
            dfs[i].loc[dfs[i]['Code'] == key , 'Code'] = val
        
        # load data into sqlite
        data = dfs[i].values.tolist()
        query = 'INSERT INTO ' + table + ' VALUES (?, ?, ?);'
        cursor.executemany(query, data)

        # save csv (additional records)
        dfs[i].to_csv(path, encoding='utf-8', index=False, mode="a")


    # clear duplicate rows
    # ref: https://dba.stackexchange.com/questions/116868/sqlite3-remove-duplicates
    cursor.execute(f'DELETE FROM {table} WHERE rowid NOT IN (SELECT min(rowid) FROM {table} GROUP BY Code)')

    return dfs

def _parse_func_helper_2013_2018(param, cursor):
    ''' 
        parsing core function (2013 ~ 2018)

        1. insert parsed data (sqlite)
        2. return pandas table, [資產負債表, 損益表, 現金流量表]
    '''
    table = table_name(param)
    id, Year, Quarter = param
    url = f'https://mops.twse.com.tw/server-java/t164sb01?step=1&CO_ID={id}&SYEAR={Year}&SSEASON={Quarter}&REPORT_ID=C'

    # create current table (sqlite)
    query = 'CREATE TABLE IF NOT EXISTS ' + table + ' (\
                Code TEXT, \
                Money BIGINT \
                );'
    print('[INFO] query: ' + query) 
    cursor.execute(query)

    # clear data (sqlite)
    query = f'DELETE FROM {table};'
    print('[INFO] query: ' + query)
    cursor.execute(query)

    # parsing
    print(f'[INFO] parsing: {url}\n')
    res = requests.get(url)
    res.encoding = 'big5'

    try:
        dfs = pd.read_html(StringIO(res.text))[1:4]
    except ValueError:
        print('[ERROR] data not available..., could be due to too many request')
        raise RequestError

    try:
        # must have three sheets
        for i in range(3):
            # must have three columns
            if len(dfs[i].columns) < 2:
                raise ValueError
    except (ValueError, IndexError):
        print('[ERROR] data not available..., could be due to too many request')
        raise RequestError
    
    # dfs[1]    資產負債表
    # dfs[2]    損益表
    # dfs[3]    現金流量表

    # clear old csv
    path = os.path.join('..', 'csv', 'financial_info', f"{table}.csv")
    if os.path.exists(path):
        os.remove(path)

    # save three financial statements
    for i in range(3):
        # drop useless columns
        dfs[i] = dfs[i].iloc[:, 0:2]
        dfs[i].columns = ['Code', 'Money']

        # drop useless rows (that has blank in "Money")
        dfs[i].dropna(subset='Money', inplace = True)

        # change Chinese Title to Code
        for key, val in Standard_Code_mapping[i].items():
            dfs[i].loc[dfs[i]['Code'] == key , 'Code'] = val

        # load data into sqlite
        data = dfs[i].values.tolist()
        query = 'INSERT INTO ' + table + ' VALUES (?, ?);'
        cursor.executemany(query, data)

        # save csv (additional records)
        dfs[i].to_csv(path, encoding='utf-8', index=False, mode="a")

    # clear duplicate rows
    # ref: https://dba.stackexchange.com/questions/116868/sqlite3-remove-duplicates
    cursor.execute(f'DELETE FROM {table} WHERE rowid NOT IN (SELECT min(rowid) FROM {table} GROUP BY Code)')

    return dfs


def _parse_func_helper_before_2013(param, cursor):
    ''' 
        (deprecated, due to data inconsistency)
        parsing core function (~2012)

        1. insert parsed data (sqlite)
        2. return pandas table, [資產負債表, 損益表, 現金流量表]
    '''
    raise Exception("🔴 Deprecated functions")
    table = table_name(param)
    id, Year, Quarter = param
    Year = str(int(Year) - 1911)    # 西元 -> 民國
    sector = INPUT_DF[INPUT_DF['CO_ID'] == id+'.TW']['Sector'].values[0]

    url = [ 'https://mops.twse.com.tw/mops/web/ajax_t05st33',  # 資產表
            'https://mops.twse.com.tw/mops/web/ajax_t05st34',  # 損益表
            'https://mops.twse.com.tw/mops/web/ajax_t05st39'  # 現金表
    ]

    # create current table (sqlite)
    query = 'CREATE TABLE IF NOT EXISTS ' + table + ' (\
                Code TEXT, \
                Money BIGINT \
                );'
    print('[QUERY] ' + query) 
    cursor.execute(query)

    # clear data (sqlite)
    query = f'DELETE FROM {table};'
    print('[QUERY] ' + query)
    cursor.execute(query)

    # check if this is a financial company
    is_fin_co = (sector == 'Financial Services')

    final_dfs = []

    # save three financial statements
    for i in range(3):
        payload = {
            'co_id': id,
            'year': Year,
            'season': '0' + Quarter,
            'firstin': '1',
            'check2858': 'Y',
            # 'checkbtn': '',
            # 'step': '1',
            # 'queryName': 'co_id',
            # 'TYPEK2': '',
            # 'inpuType': 'co_id',
            # 'off': '1',
            # 'year': 'Y',
            # 'keyword4': '',
            # 'isnew': 'false',
            # 'code1': '',
            # 'encodeURIComponent': '1',
            # 'TYPEK': 'sii'
        }

        try:
            print(f'parsing: {url[i]}\n')
            res = requests.post(url[i], data=payload)
            dfs = pd.read_html(StringIO(res.text))
        except ValueError:
            print('[Error]: data not available..., could be due to too many request')
            raise RequestError
        
        if i != 2:  # 資產表, 損益表
            # table is located at index 2
            # drop useless rows, columns
            df = dfs[-1].iloc[4:, 0:2]
            
            df.columns = ['Code', 'Money']

            # drop useless rows (that has blank in "Money")
            df.dropna(subset='Money', inplace = True)

            # remove space
            df['Code'] = df['Code'].str.replace(' ','', regex=True)

            df['Money'] = pd.to_numeric(df['Money'])

            # change Chinese Title to Standard Code
            for key, val in Standard_Code_mapping[i].items():
                df.loc[df['Code'] == key , 'Code'] = val
            
            if i == 0:  # only 資產表
                if not is_fin_co: # 一般公司，NOT 金融公司
                    query_money = lambda key: df.loc[df['Code'] == key , 'Money'].values[0]
                    # 非流動資產合計: 15XX
                    amount = query_money('1XXX') - query_money('11XX')
                    row = pd.DataFrame({'Code':['15XX'], 'Money': [amount]})
                    df = pd.concat([df, row], ignore_index=True)

                    # 非流動負債合計: 25XX
                    amount = query_money('負債總計') - query_money('21XX')
                    row = pd.DataFrame({'Code':['25XX'], 'Money': [amount]})
                    df = pd.concat([df, row], ignore_index=True)

                # 股本: 3100
                if len(df[df['Code'] == '股本']) == 0:
                    # 股本 = 普通股股本
                    df.loc[df['Code'] == '普通股股本', 'Code'] = '3100'

        else:   # 現金表
            # Note: 現金表是 raw text, not table, 因此利用string split，再linear scan

            tmp = dfs[-1].values[0][0].replace('\u3000', '')    # remove Ideographic space
            tmp = tmp.split(' ')

            tmp_dict = {'Code':[], 'Money': []}

            for i in range(len(tmp)):
                # find target values
                if tmp[i] in Standard_Code_mapping[2]:
                    code = Standard_Code_mapping[2][tmp[i]]
                    
                    if len(tmp[i+1]) == 1:  # handle bad formatted data
                        value = tmp[i+2]
                    else:
                        value = tmp[i+1]

                    if '(' in value or ')' in value:
                        value = '-' + value # negative

                    value = value.replace(',', '')
                    value = value.replace('(', '')
                    value = value.replace(')', '')
                    value = int(value) # to numeric value

                    tmp_dict['Code'].append(code)
                    tmp_dict['Money'].append(value)
                    
                    if code == 'B02700':   # handle duplicates
                        break
            df = pd.DataFrame(tmp_dict) # build df

        final_dfs.append(df)

        # load data into sqlite
        data = df.values.tolist()
        query = 'INSERT INTO ' + table + ' VALUES (?, ?);'
        cursor.executemany(query, data)

        # dfs[i].to_csv(path, encoding='utf-8', index=False)

    # clear duplicate rows
    # ref: https://dba.stackexchange.com/questions/116868/sqlite3-remove-duplicates
    cursor.execute(f'DELETE FROM {table} WHERE rowid NOT IN (SELECT min(rowid) FROM {table} GROUP BY Code)')

    return final_dfs

def _process_accumulated_data(param, cursor, Codes):
    '''
        Given codes that have accumulated data, calculate their non-accumulated data
        ex: Q4 - (Q1+Q2+Q3)
            Q3 - (Q1+Q2)
            Q2 - (Q1)

        Note: If there is no matching code (possibly missing) in the previous quarters, assume its value zero. 
            Usually, target codes will have their counterparts in the previous quarters.
    '''
    print("[INFO] Process accumulated data: ")
    print(Codes)

    table = table_name(param)
    id, Year, Quarter = param
    
    L = int(Quarter)

    # accumulated value -> non-accumulated value
    for code in Codes:
        cur_query = f'SELECT Money FROM {table} WHERE Code = "{code}"'
        cur_val = cursor.execute(cur_query).fetchone()[0]

        for season in range(1, L):
            prev_param = [id, Year, str(season)]
            prev_table = table_name(prev_param)

            prev_query = f'SELECT Money FROM {prev_table} WHERE Code = "{code}"'
            prev_val = cursor.execute(prev_query).fetchone()

            # note: sometimes, previous value does not exist
            if prev_val is not None:
                cur_val -= prev_val[0]
    
        # update values in db
        query = f'UPDATE {table} SET Money={cur_val} WHERE Code = "{code}"'
        cursor.execute(query)
