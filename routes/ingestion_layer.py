# routes/ingest.py

from fastapi import APIRouter, UploadFile, File, Body
import pandas as pd
from services.ingestion_services import ingest_dataframe
from database import engine

router = APIRouter()


@router.post("/csv")
async def ingest_csv(file: UploadFile = File(...)):
    df = pd.read_csv(file.file)
    return ingest_dataframe(df, engine)


@router.post("/json")
async def ingest_json(payload: list[dict] = Body(...)):
    df = pd.DataFrame(payload)
    return ingest_dataframe(df, engine)
