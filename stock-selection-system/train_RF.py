import random
import pickle
from tqdm import tqdm 
import statistics
import sys
import os
import configparser
import json
import pandas as pd
import numpy as np
import argparse
import joblib

from sklearn.ensemble import RandomForestRegressor

import data_input
import gen_prediction


parser = argparse.ArgumentParser()
parser.add_argument('start_Year', type=str)
parser.add_argument('start_Quarter', type=str)
parser.add_argument('end_Year', type=str)
parser.add_argument('end_Quarter', type=str)
args = parser.parse_args()


########## configuration ##########
PATH_csv = os.path.join('..', 'csv', 'ratio.csv')

data_input.start_Year = args.start_Year
data_input.start_Quarter = args.start_Quarter

end_Year = args.end_Year
end_Quarter = args.end_Quarter

PATH_wt = os.path.join("model_weights", "RF")


def _gen_train_data(X: np.ndarray, Y) -> np.ndarray:
    ''' all quarters (excluding the last quarter) '''
    N, M, K = X.shape[0], X.shape[1] - 1, X.shape[2]
    # N: num of companies
    # M: num of entries or quarters
    # K: num of features
    X_train = X[:, :-1, :].reshape((N * M, K))
    Y_train = Y[:, :-1].reshape((N * M))

    return X_train, Y_train


def _gen_test_data(X: np.ndarray) -> np.ndarray:
    return X[:, -1, :].reshape((X.shape[0], X.shape[2]))


def save_models(model):
    joblib.dump(model, os.path.join(PATH_wt, f'{end_Year}-{end_Quarter}.joblib'))


######### main script #########
def main():
    global NUM_COMPANY

    model = RandomForestRegressor(n_estimators=1000, criterion='squared_error')

    
    # 1. read data and preprocessing
    df = data_input.read_csv(PATH_csv)
    X, Y_abs, Y_rel = data_input._gen_data(df)

    # 2. generate dataset 
    co_id_list = df['CO_ID'].unique()
    NUM_COMPANY = len(co_id_list)
    
    X_train, y_train = _gen_train_data(X, Y_rel)

    # prediction, last quarters, i = N-1
    X_test = _gen_test_data(X)

    # 3. train
    model.fit(X_train, y_train)

    # 4. save model state
    save_models(model)

    # 5. predict
    gen_prediction.predict_RF(model, X_test, co_id_list, end_Year, end_Quarter)

if __name__=="__main__":
    main()