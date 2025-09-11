import fastapi
import types
from pydantic import BaseModel

app = fastapi.FastAPI()

@app.get("/")
def fn():
    return {"lol": 3}

