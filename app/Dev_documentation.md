# Backend System
## configuration
CORS permission at `main.py` 
```
origins = [
    "http://localhost:3000",
    "localhost:3000",
    "http://192.168.2.229:3000",
    "192.168.2.229:3000"
]
```

database path (`stock.db`) at `database.py` 
```
SQLALCHEMY_DATABASE_URL = "sqlite:///./Database/stock.db"
```


## run app
ref: https://www.uvicorn.org
<br>
<br>
run private server (localhost)
```
uvicorn app.main:app
```

run public server
```
sudo uvicorn app.main:app --host 140.113.231.181 --port 80
```

run development server (localhost)
```
uvicorn app.main:app --reload
```