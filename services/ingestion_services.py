import pandas as pd
import json
from datetime import datetime
import numpy as np


# Canonical Mapping

CANONICAL_MAPPING = {
    "customer_id": ["customer_id", "cust_id", "CustomerID", "id", "user_id"],
    "tenure_months": ["tenure_months", "tenure", "months_active", "TenureMonths", "subscription_months"],
    "monthly_fee": ["monthly_fee", "monthlyCharge", "monthly_cost", "fee", "MonthlySpend", "monthly_spend", "mrr"],
    "total_revenue": ["total_revenue", "revenue", "total_rev", "total_spent", "TotalSpend"],
    "lifetime_value": ["Lifetime_Value", "lifetime_value", "LTV"],
    "total_purchases": ["Total_Purchases", "total_purchases", "purchases", "TotalPurchases"],
    "membership": ["Membership_Years", "membership_years", "years_member", "HasPremiumMembership", "is_member", "membership_active"],
    "days_since_last_purchase": [
        "Days_Since_Last_Purchase", "days_since_last", "recency_days", "LastInteractionDaysAgo"
    ],
    "age": ["age", "Age", "customer_age"],
    "city": ["city", "City", "location"],
    "source": ["source_system", "system", "source"]
}


CANONICAL_COLUMNS = {
    "customer_id",
    "tenure_months",
    "monthly_fee",
    "total_revenue",
    "lifetime_value",
    "total_purchases",
    "membership",
    "days_since_last_purchase",
    "age",
    "city",
    "source"
}



# Normalize Schema

def normalize_schema(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for canonical, variants in CANONICAL_MAPPING.items():
        if canonical in df.columns:
            continue

        for col in variants:
            if col in df.columns:
                df[canonical] = df[col]
                break

    return df



# Prepare Raw Table

def prepare_raw_table(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    extra_cols = [c for c in df.columns if c not in CANONICAL_COLUMNS]
    
    if extra_cols:
        # Replace NaN with None so json.dumps produces null
        extra_df = df[extra_cols].replace({np.nan: None})
        df["extra_payload"] = extra_df.to_dict(orient="records")
        df["extra_payload"] = df["extra_payload"].apply(lambda x: json.dumps(x))
        df = df.drop(columns=extra_cols)
    else:
        df["extra_payload"] = None

    return df
    



# Persist

def persist_raw(df: pd.DataFrame, engine):
    df.to_sql(
        "raw_customer_events",
        engine,
        if_exists="append",
        index=False,
    )


# Full Ingestion Pipeline

def ingest_dataframe(df: pd.DataFrame, engine):
    """
    Prepares raw dataframe and persists to raw_customer_events.
    Stores non-canonical columns in 'extra_payload'.
    """
    # Clean column names (VERY IMPORTANT)
    df.columns = df.columns.str.strip()

    #  Normalize schema (map variants → canonical names)
    df = normalize_schema(df)

    # Ensure all canonical columns exist
    for col in CANONICAL_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df_prepared = prepare_raw_table(df)

    persist_raw(df_prepared, engine)
    return {"status": "success", "rows_inserted": len(df_prepared)}
