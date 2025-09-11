import fastapi
import types
from pydantic import BaseModel

app = fastapi.FastAPI()
#basically placeholder (will this ever be touched again? lol.)
@app.get("/")
def fn():
    return {"lol": 3}

