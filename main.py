from fastapi import FastAPI, Body
import itertools

app = FastAPI()

@app.post("/filter/case_sensitive")
async def task_1(l: list[str] = Body()):
    for a, b in itertools.combinations(l, 2):
        if a==b:
            l = [i for i in l if a.casefold() != i.casefold()]
    l = [i.lower() for i in l]
    res = set(l)
    return res