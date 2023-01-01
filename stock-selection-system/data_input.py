import pandas as pd
import numpy as np


########## configuration ##########
start_Year = None # '2013'
start_Quarter = None # '1'

# end_Year = None  #'2022'
# end_Quarter = None  #'1'


def read_csv(PATH_csv: str) -> pd.DataFrame:
    df = pd.read_csv(PATH_csv, dtype='str', keep_default_na=True)

    print('(Raw) NaN count:')
    ratio_columns = df.columns[5:-2]
    for col in ratio_columns:  # each ratio
        print(f'    {col}: ', df[col].isna().sum())

    # convert ratio & return columns to numeric type
    mask = df.columns[5:]
    df[mask] = df[mask].astype(np.float64)  

    df['Year'] = df['Year'].astype(np.int16)  # convert Year to numeric type
    df['Quarter'] = df['Quarter'].astype(np.int16)  # convert Quarter to numeric type'

    return df


############### generate numpy array from df ################
def _missing_cells(df: pd.DataFrame) -> pd.DataFrame:
    '''
        missing cell: take average of values within all intervals if other values available, else -1
    '''
    ratio_columns = df.columns[5:-2]

    for col in ratio_columns:  # each ratio
        for co_id in df['CO_ID'].unique():    # each company
            # fill -1
            if df.loc[df["CO_ID"] == co_id, col].isnull().all():
                fill_val = -1
            # fill average
            else:
                fill_val = df.loc[df["CO_ID"] == co_id, col].mean(skipna=True)

            df.loc[df["CO_ID"] == co_id, col] = df.loc[df["CO_ID"] == co_id, col].fillna(fill_val)

    return df


def _standardization(df: pd.DataFrame) -> pd.DataFrame:
    '''
        Z = (x - mean / std)
        for each ratio, take mean and std of all rows
    '''
    ratio_columns = df.columns[5:-2]

    for col in ratio_columns:  # each ratio
        avg = df.loc[:, col].mean()
        std = df.loc[:, col].std()
            
        df.loc[:, col] = (df.loc[:, col] - avg) / std

    return df

def _one_hot(df: pd.DataFrame) -> pd.DataFrame:
    ''' return only ratios & sector '''
    ratio_columns = df.columns[5:-2]

    df_sector = pd.get_dummies(df['Sector'], prefix='sec')
    df_X = pd.concat([df[ratio_columns], df_sector], axis=1)

    return df_X


def _gen_data(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    df = _missing_cells(df)
    df = _standardization(df)
    df_X = _one_hot(df)

    ratio_columns = df.columns[5:-2]
    print('\n(After preprocessing) NaN count:')
    for col in ratio_columns:  # each ratio
        print(f'    {col}: ', df[col].isna().sum())

    return _gen_X(df, df_X), _gen_Y(df, "Stock Return"), _gen_Y(df, "Relative Return")


def _gen_X(df: pd.DataFrame, df_X: pd.DataFrame) -> np.ndarray:
    co_id_list = df['CO_ID'].unique()
    N = len(co_id_list) # num of company
    M = len(df.loc[(df["CO_ID"] == co_id_list[0])]) # num of entries per company
    K = len(df_X.columns)   # num of features

    X = np.zeros((N, M, K), dtype=float)    # Shape

    for i, co_id in enumerate(co_id_list):
        Y = int(start_Year)
        Q = int(start_Quarter)
        for j in range(M):
            # assign one row (one quarter)
            X[i, j] = df_X.loc[(df["CO_ID"] == co_id) & (df["Year"] == Y) & (df["Quarter"] == Q)].values[0]

            Y += (Q == 4)
            Q = (Q%4) + 1

    return X


def _gen_Y(df: pd.DataFrame, col) -> np.ndarray:        
    co_id_list = df['CO_ID'].unique()
    N = len(co_id_list) # num of company
    M = len(df.loc[(df["CO_ID"] == co_id_list[0])]) # num of entries per company

    Y = np.zeros((N, M), dtype=float)   # Shape

    for i, co_id in enumerate(co_id_list):
        Y[i] = df.loc[(df["CO_ID"] == co_id), col].values

    return Y
