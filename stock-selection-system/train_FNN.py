# must run gen_csv.py before this file

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

from dataset import dataset
from model import FNN
import torch.nn as nn
import torch
from torch.utils.data import DataLoader

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

NUM_COMPANY = None

device = torch.device("cpu")

PATH_wt = os.path.join("model_weights", "FNN")

########## model dict ##########
best_mse = {'mse':10000, 'return':0, 'model':None, 'first_layer':None, 'second_layer':None, 'third_layer':None, 'lr':None, 'last_lr':None}
second_best_mse = {'mse':10000, 'return':0, 'model':None, 'first_layer':None, 'second_layer':None, 'third_layer':None, 'lr':None, 'last_lr':None}
third_best_mse = {'mse':10000, 'return':0, 'model':None, 'first_layer':None, 'second_layer':None, 'third_layer':None, 'lr':None, 'last_lr':None}

best_return = {'mse':0, 'return':-10000, 'model':None, 'first_layer':None, 'second_layer':None, 'third_layer':None, 'lr':None, 'last_lr':None}
second_best_return = {'mse':0, 'return':-10000, 'model':None, 'first_layer':None, 'second_layer':None, 'third_layer':None, 'lr':None, 'last_lr':None}
third_best_return = {'mse':0, 'return':-10000, 'model':None, 'first_layer':None, 'second_layer':None, 'third_layer':None, 'lr':None, 'last_lr':None}

best_model = None

############### generate dataloader ################
def _gen_train_index(end, test_len=4) -> np.ndarray:
    '''
        train1 & val: 0 ~ end-5
        train1: 4/5
        val: 1/5 (randomly sample from last 2/3)
        train2: train1 + end-4 ~ end-1 quarters (excluding val)
    '''
    N_step1 = end - test_len
    N_step2 = end

    val_idx = np.random.choice(range(N_step1*2//3, N_step1), size=N_step1//5, replace=False).tolist()

    train_idx_step1 = list(set(range(N_step1)) - set(val_idx))
    train_idx_step2 = train_idx_step1 + list(range(N_step1, N_step2))

    return val_idx, train_idx_step1, train_idx_step2


def _gen_data_loader(X: np.ndarray, Y: np.ndarray, val_idx, train_idx_step1, train_idx_step2):
    val_dataset = dataset('FNN', X, Y, val_idx)
    train_step1_dataset = dataset('FNN', X, Y, train_idx_step1)
    train_step2_dataset = dataset('FNN', X, Y, train_idx_step2)

    val_loader = DataLoader(val_dataset, batch_size=16, shuffle=False)
    train_step1_loader = DataLoader(train_step1_dataset, batch_size=16, shuffle=True)
    train_step2_loader = DataLoader(train_step2_dataset, batch_size=16, shuffle=True)
    
    return val_loader, train_step1_loader, train_step2_loader


############### generate test data ################
def _gen_test_step1_data(X: np.ndarray, Y_rel, Y_abs, test_len=4) -> np.ndarray:
    ''' last 4 quarters (excluding the last quarter) '''
    end = X.shape[1] - 1
    l = test_len
    N, M, K = X.shape[0], test_len, X.shape[2]
    # N: num of companies
    # M: num of entries or quarters
    # K: num of features

    # X_test = np.zeros((N * M, K), dtype=float)
    # Y_test_rel = np.zeros((N * M), dtype=float)
    # Y_test_abs = np.zeros((N * M), dtype=float)

    # k = 0
    # for i in range(N):
    #     for j in range(end-test_len, end):
    #         X_test[k] = X[i, j]
    #         Y_test_rel[k] = Y_rel[i, j]
    #         Y_test_abs[k] = Y_abs[i, j]
    #         k += 1
    X_test = X[:, end-l:end, :].reshape((N * M, K))
    Y_test_rel = Y_rel[:, end-l:end].reshape((N * M))
    Y_test_abs = Y_abs[:, end-l:end].reshape((N * M))

    return X_test, Y_test_rel, Y_test_abs

def _gen_test_data(X: np.ndarray) -> np.ndarray:
    return X[:, -1, :].reshape((X.shape[0], X.shape[2]))

#generate company_buy in step1
def generate_company_buy(model, X_test: np.ndarray, pick_interval):
    global device

    X_test = torch.Tensor(X_test)
    X_test = X_test.to(device)

    y_predict = model(X_test)
    y_predict = y_predict.squeeze()
    y_predict = y_predict.tolist()
    company_buy = []
    for i in range(4):
        ith_quarter_predict_price = []
        for j in range(NUM_COMPANY):
            ith_quarter_predict_price.append(y_predict[i+4*j])     

        ith_quarter_predict_price_sorted = ith_quarter_predict_price.copy()
        sorted_index = sorted(range(len(ith_quarter_predict_price)), key=lambda k: ith_quarter_predict_price[k])
        ith_quarter_predict_price_sorted.sort()

        index = 1
        count = 0
        while index <= NUM_COMPANY:
            if i == 0:
                company_buy.append([])

            start = index
            end = index + pick_interval - 1 if index + pick_interval - 1 <= NUM_COMPANY else NUM_COMPANY
            z = []
            for g in range(start, end + 1):
                z.append(sorted_index[-g])
            company_buy[count].append(z)

            count += 1
            index += pick_interval
    return company_buy


# calulate return for model selection in step1
def cal_return(company_buy, y_test: np.ndarray): 
    pick_number = len(company_buy[0])
    num_quarters = len(company_buy)
    portfolio_return = 1.0    
    for i in range(num_quarters):
        quarterly_return = 0.0
        for j in range(pick_number):
            quarterly_return += y_test[company_buy[i][j]*num_quarters+i]
        portfolio_return *= (1 + quarterly_return / pick_number)     
    return portfolio_return



def train_util(model, train_loader, val_loader, criterion, optimizer, scheduler):
    global device

    #train
    total_loss = 0
    for x, y in train_loader:
        x = x.to(device)
        y = y.to(device)
        output = model(x)
        loss = criterion(output, y)
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()
        total_loss += loss.item()
    avg_loss = total_loss / len(train_loader.dataset)
    print("train loss:{:.6f}".format(avg_loss), end = '    ')

    # validation
    total_loss = 0
    with torch.no_grad():
        for x, y in val_loader:
            x = x.to(device)
            y = y.to(device)
            output = model(x)
            total_loss += criterion(output, y).item()
    avg_loss = total_loss / len(val_loader.dataset)
    scheduler.step(avg_loss)
    print("val loss:{:.6f}".format(avg_loss))


############# train step 1 #############
def train_step1(train_step1_loader, val_loader, num_features, X_test, Y_test_rel, Y_test_abs):
    global best_mse, second_best_mse, third_best_mse
    global best_return, second_best_return, third_best_return
    global device

    X_test = torch.Tensor(X_test)

    ###### hyperparameters ######
    criterion = nn.MSELoss(reduction='sum')
    epoch = 50

    first_layers = [20, 30, 40]
    # second_layers = [0.5, 0.75, 1, 1.25, 1.5]
    second_layers = [0.75, 1, 1.25]
    # third_layers = [0.5, 0.75, 1, 1.25, 1.5]
    third_layers = [0.75, 1, 1.25]
    # lrs = [0.01, 0.001, 0.0001]   # FIXME: should we search lr?
    lr = 0.001


    # n = len(first_layers) * len(second_layers) * len(third_layers)* len(lrs)
    n = len(first_layers) * len(second_layers) * len(third_layers)
    pbar = tqdm(range(n))
    # for lr in lrs:
    for first_layer in first_layers:
        for second_layer in second_layers:     
            for third_layer in third_layers:
                model = FNN(num_features, first_layer, int(first_layer * second_layer), int(first_layer * second_layer * third_layer))
                model = model.to(device)

                optimizer = torch.optim.Adam(model.parameters(), lr=lr)
                scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer, factor=0.1)

                #train
                for _ in range(epoch):
                    train_util(model, train_step1_loader, val_loader, criterion, optimizer, scheduler)

                # calculate mse
                pred = model(X_test)
                pred = pred.squeeze()
                pred = pred.tolist()
                mse = 0.0
                y = Y_test_rel
                for i in range(len(y)):
                    mse += ((y[i] - pred[i]) * (y[i] - pred[i]))
                mse /= len(y)

                # calculate return
                company_buy = generate_company_buy(model, X_test, pick_interval=20)
                _return = cal_return(company_buy[0], Y_test_abs)

                if mse < best_mse['mse']:
                    new_best_mse = best_mse.copy()
                    temp = second_best_mse
                    second_best_mse = best_mse
                    third_best_mse = temp
                    best_mse = new_best_mse

                    best_mse['mse'] = mse
                    best_mse['return'] = _return
                    best_mse['model'] = model
                    best_mse['first_layer'] = first_layer
                    best_mse['second_layer'] = int(first_layer * second_layer)
                    best_mse['third_layer'] = int(first_layer * second_layer * third_layer)
                    best_mse['lr'] = lr
                    best_mse['last_lr'] = scheduler.optimizer.param_groups[0]['lr']

                elif mse < second_best_mse['mse']:
                    third_best_mse = second_best_mse
                    second_best_mse = second_best_mse.copy()
                    second_best_mse['mse'] = mse
                    second_best_mse['return'] = _return
                    second_best_mse['model'] = model
                    second_best_mse['first_layer'] = first_layer
                    second_best_mse['second_layer'] = int(first_layer * second_layer)
                    second_best_mse['third_layer'] = int(first_layer * second_layer * third_layer)
                    second_best_mse['lr'] = lr
                    second_best_mse['last_lr'] = scheduler.optimizer.param_groups[0]['lr']
                
                elif mse < third_best_mse['mse']:
                    third_best_mse['mse'] = mse
                    third_best_mse['return'] = _return
                    third_best_mse['model'] = model
                    third_best_mse['first_layer'] = first_layer
                    third_best_mse['second_layer'] = int(first_layer * second_layer)
                    third_best_mse['third_layer'] = int(first_layer * second_layer * third_layer)
                    third_best_mse['lr'] = lr
                    third_best_mse['last_lr'] = scheduler.optimizer.param_groups[0]['lr']
                
                # if _return > best_return['return']:
                #     new_best_return = best_return.copy()
                #     temp = second_best_return
                #     second_best_return = best_return
                #     third_best_return = temp
                #     best_return = new_best_return

                #     best_return['mse'] = mse
                #     best_return['return'] = _return
                #     best_return['model'] = model
                #     best_return['first_layer'] = first_layer
                #     best_return['second_layer'] = int(first_layer * second_layer)
                #     best_return['third_layer'] = int(first_layer * second_layer * third_layer)
                #     best_return['lr'] = lr
                #     best_return['last_lr'] = scheduler.optimizer.param_groups[0]['lr']
                
                # elif _return > second_best_return['return']:
                #     third_best_return = second_best_return
                #     second_best_return = second_best_return.copy()
                #     second_best_return['mse'] = mse
                #     second_best_return['return'] = _return
                #     second_best_return['model'] = model
                #     second_best_return['first_layer'] = first_layer
                #     second_best_return['second_layer'] = int(first_layer * second_layer)
                #     second_best_return['third_layer'] = int(first_layer * second_layer * third_layer)
                #     second_best_return['lr'] = lr
                #     second_best_return['last_lr'] = scheduler.optimizer.param_groups[0]['lr']
                
                # elif _return > third_best_return['return']:
                #     third_best_return['mse'] = mse
                #     third_best_return['return'] = _return
                #     third_best_return['model'] = model
                #     third_best_return['first_layer'] = first_layer
                #     third_best_return['second_layer'] = int(first_layer * second_layer)
                #     third_best_return['third_layer'] = int(first_layer * second_layer * third_layer)
                #     third_best_return['lr'] = lr
                #     third_best_return['last_lr'] = scheduler.optimizer.param_groups[0]['lr']

                pbar.update()


############# train step 2 #############
def train_step2(train_step2_loader, val_loader):
    global best_mse, second_best_mse, third_best_mse
    global best_return, second_best_return, third_best_return

    def finetune(model_dict, train_step2_loader, val_loader):
        epoch = 50
        criterion = nn.MSELoss(reduction='sum')
        optimizer = torch.optim.Adam(model_dict['model'].parameters(), lr=model_dict['last_lr'])
        scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer, factor=0.1)

        for _ in range(epoch):
            train_util(model_dict['model'], train_step2_loader, val_loader, criterion, optimizer, scheduler)

    #finetune best_mse['model']
    finetune(best_mse, train_step2_loader, val_loader)

    #finetune second_best_mse['model']
    finetune(second_best_mse, train_step2_loader, val_loader)

    #finetune third_best_mse['model']
    finetune(third_best_mse, train_step2_loader, val_loader)

    # #finetune best_return['model']
    # finetune(best_return, train_step2_loader, val_loader)

    # #finetune second_best_return['model']
    # finetune(second_best_return, train_step2_loader, val_loader)

    # #finetune third_best_return['model']
    # finetune(third_best_return, train_step2_loader, val_loader)


############# save models #############
def save_models():
    global best_mse, second_best_mse, third_best_mse
    global best_return, second_best_return, third_best_return
    global best_model

    # 1. save minimum mse
    idx = np.argmin([best_mse['mse'], second_best_mse['mse'], third_best_mse['mse']])
    if idx == 0:
        model_dict = best_mse
    elif idx == 1:
        model_dict = second_best_mse
    else:
        model_dict = third_best_mse
    print(model_dict)

    torch.save(model_dict['model'].state_dict(), os.path.join(PATH_wt, f'{end_Year}-{end_Quarter}.pth'))

    best_model = model_dict['model']

    model_info = {
        'first_layer': model_dict['first_layer'],
        'second_layer': model_dict['second_layer'],
        'third_layer': model_dict['third_layer']
    }

    with open(os.path.join(PATH_wt, f'{end_Year}-{end_Quarter}-model-info.pickle'), "wb") as fp:
        pickle.dump(model_info, fp)

    # 2. save maximum return
    # idx = np.argmax([best_return['return'], second_best_return['return'], third_best_return['return']])
    # if idx == 0:
    #     model_dict = best_return
    # elif idx == 1:
    #     model_dict = second_best_return
    # else:
    #     model_dict = third_best_return
    # print(model_dict)
    # torch.save(model_dict['model'].state_dict(), os.path.join(PATH_wt, f'ret-{end_Year}-{end_Quarter}.pth'))

    #save best_mse
    # save(best_mse, 'best_mse')

    # #save second_best_mse
    # save(second_best_mse, 'second_best_mse')

    # #save third_best_mse
    # save(third_best_mse, 'third_best_mse')

    # #save best_return
    # save(best_return, 'best_return')

    # #save second_best_return
    # save(second_best_return, 'second_best_return')

    # #save third_best_return
    # save(third_best_return, 'third_best_return')


######### main script #########
def main():
    global NUM_COMPANY
    
    # 1. read data and preprocessing
    df = data_input.read_csv(PATH_csv)
    X, Y_abs, Y_rel = data_input._gen_data(df)

    # 2. generate dataset & dataloaders
    co_id_list = df['CO_ID'].unique()
    NUM_COMPANY = len(co_id_list)

    co_id_dummy = df['CO_ID'][0]
    N = len(df.loc[(df["CO_ID"] == co_id_dummy)]) # num of entries per company (num of quarter)
    num_features = X.shape[2]

    # last index is for test data, exclude that
    val_idx, train_idx_step1, train_idx_step2 = _gen_train_index(end=N-1, test_len=4)
    # print(val_idx, train_idx_step1, train_idx_step2) # DEBUG

    val_loader, train_step1_loader, train_step2_loader \
        = _gen_data_loader(X, Y_rel, val_idx, train_idx_step1, train_idx_step2)

    # test_step1, 4 quarters, i = N-6 ~ N-2
    X_test, Y_test_rel, Y_test_abs = _gen_test_step1_data(X, Ｙ_rel, Y_abs, test_len=4)

    # prediction, last quarters, i = N-1
    X_test = _gen_test_data(X)

    # 3. train step #1 & select top models
    train_step1(train_step1_loader, val_loader, num_features, X_test, Y_test_rel, Y_test_abs)

    # 4. train step #2
    train_step2(train_step2_loader, val_loader)

    # 5. save model weights
    save_models()

    #6. predict target quarter
    gen_prediction.predict_FNN(best_model, X_test, co_id_list, end_Year, end_Quarter)

if __name__=="__main__":
    main()