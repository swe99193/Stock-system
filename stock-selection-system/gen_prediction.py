import torch
import pickle
import os

PATH_pred = os.path.join("predictions")

def predict_FNN(model, X_test, co_id_list, end_Year, end_Quarter):
    X_test = torch.Tensor(X_test)

    pred = model(X_test)
    pred = pred.squeeze().tolist()

    # sorted index, descending predicted return
    pred = sorted(range(len(pred)), key=lambda k: -pred[k])

    co_id_rank = [co_id_list[idx] for idx in pred]

    with open(os.path.join(PATH_pred, "FNN", f"{end_Year}-{end_Quarter}-company-rank.pickle"), "wb") as fp:
        pickle.dump(co_id_rank, fp)

def predict_RF(model, X_test, co_id_list, end_Year, end_Quarter):
    
    pred = model.predict(X_test)

    # sorted index, descending predicted return
    pred = sorted(range(len(pred)), key=lambda k: -pred[k])

    co_id_rank = [co_id_list[idx] for idx in pred]

    with open(os.path.join(PATH_pred, "RF", f"{end_Year}-{end_Quarter}-company-rank.pickle"), "wb") as fp:
        pickle.dump(co_id_rank, fp)