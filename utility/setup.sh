#!/bin/bash

echo "install dependencies:"
echo "fastapi 0.85.2"
echo "uvicorn[standard]"
echo "sqlalchemy"
conda install fastapi=0.85.2
conda install "uvicorn[standard]"
conda install sqlalchemy=1.4.32

# at stock-system/
python -m uvicorn app.main:app --host 140.113.207.34
python -m uvicorn app.main:app

# at stock-system/frontend/my-app
npm start
