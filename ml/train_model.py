import os
import time
from math import ceil
import pandas as pd
import joblib
import urllib.parse
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.ensemble import RandomForestRegressor
import math


# ---------------- DB Credentials ----------------
DB_USER = "root"
DB_PASS = urllib.parse.quote_plus("Pankaj@123")
DB_HOST = "localhost"
DB_PORT = "3306"
DB_NAME = "mycrm_db"
db_url = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)

os.makedirs("ml", exist_ok=True)

# =========================
# CONSUMER TRAINING (unchanged)
# =========================
def train_consumer():
    print("📥 Loading data from consumer_pricing...")
    df = pd.read_sql("SELECT * FROM consumer_pricing", engine)

    df['market'] = df['market'].astype(str).str.upper().str.strip()
    df.loc[df['market'] != 'USA', 'market'] = 'INTERNATIONAL'
    

    def to_int_bucket(x):
        if pd.isna(x):
            return None
        try:
            return int(ceil(float(x)))
        except:
            return None

    df['incidence_rate'] = df['incidence_rate'].apply(to_int_bucket)
    df['loi_minutes'] = df['loi_minutes'].apply(to_int_bucket)

    df = df.dropna(subset=['market', 'incidence_rate', 'loi_minutes', 'price'])
    df['incidence_rate'] = df['incidence_rate'].astype(int)
    df['loi_minutes'] = df['loi_minutes'].astype(int)
    df['price'] = df['price'].astype(float)

    # Lookup
    lookup = df.set_index(['market', 'incidence_rate', 'loi_minutes'])['price'].to_dict()

    # Buckets
    ir_buckets = sorted(df['incidence_rate'].unique().tolist())
    loi_buckets = sorted(df['loi_minutes'].unique().tolist())

    # ML
    X_market = pd.get_dummies(df['market'], prefix='market')
    X_numeric = df[['incidence_rate', 'loi_minutes']].reset_index(drop=True)
    X = pd.concat([X_market.reset_index(drop=True), X_numeric], axis=1)
    y = df['price']

    model_features = X.columns.tolist()
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = RandomForestRegressor(n_estimators=300, max_depth=15, random_state=42, n_jobs=-1)
    start = time.time()
    model.fit(X_train, y_train)
    train_time = round(time.time() - start, 2)

    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    # Save artifacts
    joblib.dump(model, "ml/consumer_pricing_model.pkl")
    joblib.dump(model_features, "ml/consumer_pricing_features.pkl")
    joblib.dump(lookup, "ml/consumer_pricing_lookup.pkl")
    joblib.dump({'ir_buckets': ir_buckets, 'loi_buckets': loi_buckets}, "ml/consumer_pricing_buckets.pkl")

    print(f"✅ Consumer Model trained (rows={len(df)}, mse={mse:.4f}, r2={r2:.4f})")

# =========================
# B2B TRAINING (NEW STRUCTURE)
# =========================
def train_b2b():
    print("📥 Loading data from qlab_b2b_pricing...")
    df = pd.read_sql("SELECT * FROM qlab_b2b_pricing", engine)

    df['country_name'] = df['country_name'].astype(str).str.upper().str.strip()

    records = []
    for _, row in df.iterrows():
        records.append({
            "country_name": row['country_name'],
            "loi_min": int(row['loi_min']),
            "loi_max": int(row['loi_max']),
            "incidence_min": int(row['incidence_min']),
            "incidence_max": int(row['incidence_max']),
            "price": float(row['price'])
        })

    joblib.dump(records, "ml/qlab_b2b_pricing_lookup.pkl")
    print(f"✅ B2B lookup saved (rows={len(records)})")



def _ensure_range(val):
    """Ensure parsed value is always a (min, max) tuple."""
    if isinstance(val, tuple):
        return val
    if val is None:
        return None
    return (val, val)  # wrap int into a tuple


def train_acuity_b2b():
    print("📥 Loading data from b2b_cpi_pricing_acquity...")
    df = pd.read_sql("SELECT * FROM b2b_cpi_pricing_acquity", engine)

    # normalize country names
    df['country_name'] = df['country_name'].astype(str).str.upper().str.strip()

    records = []
    for _, row in df.iterrows():
        loi_min = int(row['loi_min'])
        loi_max = int(row['loi_max'])

        # 🔑 If loi_max == 40 → this is your "30–40+" bucket, extend it
        if loi_max == 40:
            loi_max = 999  

        records.append({
            "country_name": row['country_name'],
            "loi_min": loi_min,
            "loi_max": loi_max,
            "incidence_min": int(row['incidence_min']),
            "incidence_max": int(row['incidence_max']),
            "price": float(row['price'])
        })

    # save lookup
    joblib.dump(records, "ml/acuity_b2b_pricing_lookup.pkl")
    print(f"✅ Acuity B2B lookup saved (rows={len(records)})")


def train_acuity_b2c():
    print("📥 Loading data from b2c_cpi_pricing_acquity...")
    df = pd.read_sql("SELECT * FROM b2c_cpi_pricing_acquity", engine)

    # normalize country names
    df['country_name'] = df['country_name'].astype(str).str.upper().str.strip()

    records = []
    for _, row in df.iterrows():
        loi_min = int(row['loi_min'])
        loi_max = int(row['loi_max'])

        # 🔑 If loi_max == 40 → this is your "30–40+" bucket, extend it
        if loi_max == 40:
            loi_max = 999  

        records.append({
            "country_name": row['country_name'],
            "loi_min": loi_min,
            "loi_max": loi_max,
            "incidence_min": int(row['incidence_min']),
            "incidence_max": int(row['incidence_max']),
            "price": float(row['price'])
        })

    # save lookup
    joblib.dump(records, "ml/acuity_b2c_pricing_lookup.pkl")
    print(f"✅ Acuity B2C lookup saved (rows={len(records)})")


def train_b2b_with_client():
    print("📥 Loading data from survey_pricing...")
    df = pd.read_sql("SELECT * FROM survey_pricing", engine)

    # normalize client names
    df['client_name'] = df['client_name'].astype(str).str.lower().str.strip()

    records = []
    for _, row in df.iterrows():
        records.append({
            "client_name": row['client_name'],
            "min_cpi": float(row['min_cpi']),
            "max_cpi": float(row['max_cpi']),
            "dir_premium": float(row['dir_premium']),
            "clevel_premium": float(row['clevel_premium']),
        })

    # save lookup
    joblib.dump(records, "ml/b2b_with_client_pricing_lookup.pkl")
    print(f"✅ B2B with client lookup saved (rows={len(records)})")



if __name__ == "__main__":
    train_consumer()
    train_b2b()
    train_acuity_b2b()
    train_acuity_b2c()
    train_b2b_with_client()


