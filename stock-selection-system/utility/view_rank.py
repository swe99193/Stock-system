import pickle
import os

PATH_pred = os.path.join("predictions", "FNN")

Year = 2019
Quarter = 1

with open(os.path.join(PATH_pred, f"{Year}-{Quarter}-company-rank.pickle"), "rb") as fp:
    co_id_rank = pickle.load(fp)
    print(co_id_rank)