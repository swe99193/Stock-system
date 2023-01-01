#!/bin/bash

conda activate StockSys-env

mkdir -p model_weights/FNN
mkdir -p model_weights/RF

mkdir -p predictions/FNN
mkdir -p predictions/RF

for i in {2019..2021}
do
    for j in {1..4}
    do
        python3 gen_csv_script.py 2013 1 "$i" "$j"
        python3 train_FNN.py 2013 1 "$i" "$j"
        python3 train_RF.py 2013 1 "$i" "$j"
        python3 gen_portfolio.py "$i" "$j" FNN "equal weight"
        python3 gen_portfolio.py "$i" "$j" RF "equal weight"
    done
done

python3 gen_csv_script.py 2013 1 2022 1
python3 train_FNN.py 2013 1 2022 1
python3 train_RF.py 2013 1 2022 1
python3 gen_portfolio.py 2022 1 FNN "equal weight"
python3 gen_portfolio.py 2022 1 RF "equal weight"