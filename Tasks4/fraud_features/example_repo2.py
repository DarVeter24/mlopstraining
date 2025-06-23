from feast import Entity, FeatureView, Field, FileSource
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Int64, Float32
from feast.data_format import ParquetFormat
from feast.value_type import ValueType
from datetime import timedelta
import pandas as pd
import os

# ───────── переменные окружения ─────────
os.environ.update(
    AWS_ACCESS_KEY_ID="admin",
    AWS_SECRET_ACCESS_KEY="password",
    AWS_ENDPOINT_URL="http://192.168.31.201:9000",
)

# ───────── источник данных ─────────
tx_source = FileSource(
    name="tx_parquet_source",
    path="s3://otus/clean/fraud_transactions_fixed_new.parquet",
    timestamp_field="tx_datetime",
    file_format=ParquetFormat(),
    s3_endpoint_override="http://192.168.31.201:9000",
)

# ───────── entity ─────────
customer = Entity(
    name="customer_id",
    join_keys=["customer_id"],
    value_type=ValueType.INT64,         # <‑‑ добавили
)

# ───────── feature‑view с транзакциями ─────────
tx_features_fv = FeatureView(
    name="tx_features",
    entities=[customer],
    ttl=timedelta(days=365),
    schema=[
        Field(name="tx_amount",       dtype=Float32),
        Field(name="tx_time_seconds", dtype=Int64),
        Field(name="tx_time_days",    dtype=Int64),
    ],
    online=True,
    source=tx_source,
)

# ───────── feature‑view с метками ─────────
fraud_labels_fv = FeatureView(
    name="fraud_labels",
    entities=[customer],
    ttl=timedelta(days=365),
    schema=[
        Field(name="tx_fraud",          dtype=Int64),
        Field(name="tx_fraud_scenario", dtype=Int64),
    ],
    online=True,
    source=tx_source,
)

# ───────── on‑demand feature‑view ─────────
@on_demand_feature_view(
    sources=[tx_features_fv],
    schema=[Field(name="is_large_tx", dtype=Int64)],
)
def tx_flags(df: pd.DataFrame) -> pd.DataFrame:
    df["is_large_tx"] = (df["tx_amount"] > 1_000).astype(int)
    return df[["is_large_tx"]]