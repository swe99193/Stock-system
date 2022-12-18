from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from . import crud, models, schemas
from .database import engine

from .routers import (
    company_data
)


app = FastAPI()

origins = [
    "http://localhost:3000",
    "localhost:3000",
    "http://192.168.2.229:3000",
    "192.168.2.229:3000"
]

# Cross Origin Resource Sharing (CORS), Frontend (React.js)
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


models.Base.metadata.create_all(bind=engine)
app.include_router(company_data.router)


# @app.get("/")
# async def root():
#     return {"message": "Hello World"}

