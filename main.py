
from fastapi import FastAPI
from routes.ingestion_layer import router as ingest_router

app = FastAPI(title="Customer Ingestion Service")

app.include_router(ingest_router, prefix="/ingestion_layer")