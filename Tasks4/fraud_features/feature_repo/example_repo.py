# feature_repo/example_repo.py  (чтение из ЛОКАЛЬНОГО parquet‑файла)

from datetime import timedelta
import pandas as pd

from feast import Entity, FeatureView, Field, FileSource
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Int64, Float32
from feast.value_type import ValueType
from feast.data_format import ParquetFormat

# ───────── 1. Источник ─────────
tx_source = FileSource(
    name="tx_parquet_source",
    path="data/fraud_transactions_fixed_new_fixed.parquet",  # локальный файл
    timestamp_field="tx_datetime",
    file_format=ParquetFormat(),
)

# ───────── 2. Entity ─────────
customer = Entity(
    name="customer_id",
    join_keys=["customer_id"],
    value_type=ValueType.INT64,           # 💡 ValueType.INT64 тоже подойдёт
)

# ───────── 3. FeatureView #1 ─────────
tx_features_fv = FeatureView(
    name="tx_features",
    entities=[customer],
    ttl=timedelta(days=365),
    schema=[
        Field(name="tx_amount",       dtype=Float32),
        Field(name="tx_time_seconds", dtype=Int64),
        Field(name="tx_time_days",    dtype=Int64),
    ],
    source=tx_source,
)

# ───────── 4. FeatureView #2 ─────────
fraud_labels_fv = FeatureView(
    name="fraud_labels",
    entities=[customer],
    ttl=timedelta(days=365),
    schema=[
        Field(name="tx_fraud",          dtype=Int64),
        Field(name="tx_fraud_scenario", dtype=Int64),
    ],
    source=tx_source,
)

# ───────── 5. On‑demand FeatureView ─────────
@on_demand_feature_view(
    sources=[tx_features_fv],
    schema=[Field(name="is_large_tx", dtype=Int64)],
)
def tx_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Флаг ‘крупной’ транзакции (>1000)."""
    df["is_large_tx"] = (df["tx_amount"] > 1_000).astype(int)
    return df[["is_large_tx"]]
