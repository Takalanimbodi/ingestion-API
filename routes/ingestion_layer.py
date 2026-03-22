# routes/ingest.py
from fastapi import APIRouter, UploadFile, File, Body
import pandas as pd
import requests
import os
from sqlalchemy import text
from services.ingestion_services import ingest_dataframe
from database import engine

router = APIRouter()

def run_pipeline():
    """Run validation then clustering in correct order"""
    
    with engine.connect() as conn:
        # Step 1 — Validate first
        conn.execute(text("SELECT run_scheduled_validation()"))
        conn.commit()

        # Step 2 — Check clean views have data
        result_a = conn.execute(
            text("SELECT COUNT(*) FROM clean_customer_model_a")
        ).scalar()
        result_b = conn.execute(
            text("SELECT COUNT(*) FROM clean_customer_model_b")
        ).scalar()

        # Step 3 — Get API secret from system_secrets table
        api_secret = conn.execute(
            text("SELECT value FROM system_secrets WHERE key = 'API_SECRET'")
        ).scalar()

    # Step 4 — Only trigger clustering if clean data exists
    if result_a > 0 or result_b > 0 and api_secret:
        clustering_url = "https://customer-clustering-api-4.onrender.com/run_clustering"
        headers = {"x-api-secret": api_secret}
        requests.post(clustering_url, headers=headers, timeout=60)

@router.post("/csv")
async def ingest_csv(file: UploadFile = File(...)):
    df = pd.read_csv(file.file)
    result = ingest_dataframe(df, engine)
    run_pipeline()
    return result

@router.post("/json")
async def ingest_json(payload: list[dict] = Body(...)):
    df = pd.DataFrame(payload)
    result = ingest_dataframe(df, engine)
    run_pipeline()
    return result
