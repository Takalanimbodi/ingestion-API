import os
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(
    DATABASE_URL,
    poolclass=NullPool   # REQUIRED for Supabase pooler
)
