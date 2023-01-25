from fastapi import FastAPI, File, UploadFile, Body, HTTPException, Query
import itertools, os
import pandas as pd
import os.path
from fastapi.responses import FileResponse

os.environ["DATA_DIR"] = 'D:/Git/reps/python-fastapi/data'
DATA_DIR = os.getenv("DATA_DIR")
app = FastAPI()

@app.post("/filter/case_sensitive")
async def task_1(l: list[str] = Body()):
    for a, b in itertools.combinations(l, 2):
        if a==b:
            l = [i for i in l if a.casefold() != i.casefold()]
    l = [i.lower() for i in l]
    res = set(l)
    return res

@app.post("/upload/file_name")
async def task_2_1(file_list: list[UploadFile] = File()):
    error_files = []
    for i in file_list:
        if (i.content_type != "text/csv") & (i.content_type != "application/json"):
            error_files.append(i.filename)
        if error_files:
            raise HTTPException(status_code=415, detail=f"wrong type of the file ({error_files})")
        else:
            if i.content_type == "text/csv":
                if os.stat(f'{DATA_DIR}/file_name.csv').st_size == 0:
                    file_name = open(f'{DATA_DIR}/file_name.csv', 'wb')
                    contents = await i.read() 
                    file_name.write(contents)
                    file_name.close()
                else:
                    file_csv = open(f'{DATA_DIR}/{i.filename}', 'r')
                    csv_pd1 = pd.read_csv(f'{DATA_DIR}/{i.filename}', header=0, sep = ";", encoding='utf-8')
                    file_csv.close()

                    file_name = open(f'{DATA_DIR}/file_name.csv', 'r')
                    csv_pd2 = pd.read_csv(f'{DATA_DIR}/file_name.csv', header=0, sep = ";", encoding='utf-8')
                    file_name.close()

                    outputq = pd.merge(csv_pd1, csv_pd2, how="outer", sort=True)
                    outputq.to_csv(f'{DATA_DIR}/file_name.csv', header = True, encoding='utf-8', index=False, sep=";")
            
            if i.content_type == "application/json":
                if os.stat(f'{DATA_DIR}/file_name.csv').st_size == 0:
                    json_pd = pd.read_json(i.file)
                    json_pd.to_csv(f'{DATA_DIR}/temp.csv', encoding='utf-8', index=False, sep=";")
                    json_csv = open(f'{DATA_DIR}/temp.csv', 'r')
                    contents = json_csv.read()
                    json_csv.close()

                    file_name = open(f'{DATA_DIR}/file_name.csv', 'w')
                    file_name.write(contents)
                    file_name.close()
                else:
                    json_pd = pd.read_json(i.file)
                    json_pd.to_csv(f'{DATA_DIR}/temp.csv', encoding='utf-8', index=False, sep=";")

                    json_file = open(f'{DATA_DIR}/temp.csv', 'r')
                    json_pd = pd.read_csv(f'{DATA_DIR}/temp.csv', header=0, sep = ";", encoding='utf-8')
                    json_file.close()

                    file_name = open(f'{DATA_DIR}/file_name.csv', 'r')
                    csv_pd = pd.read_csv(f'{DATA_DIR}/file_name.csv', header=0, sep = ";", encoding='utf-8')

                    output = pd.merge(json_pd, csv_pd, how="outer", sort=True)
                    output.to_csv(f'{DATA_DIR}/file_name.csv', header = True, encoding='utf-8', index=False, sep=";")
            return "success"

@app.post("/load/")
async def task_2_2(find_filename: str = Query()):
    files = []
    path = f'{DATA_DIR}/{find_filename}'
    if os.path.isfile(path):
        return FileResponse(path)
    else:
        raise HTTPException(status_code=404, detail=f"file {find_filename} is not uploaded")