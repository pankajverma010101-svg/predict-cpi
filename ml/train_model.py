import os
import re
import numpy as np
import pandas as pd
import joblib
import urllib.parse
import time
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
from sqlalchemy import create_engine

# ✅ DB Credentials
DB_USER = "root"
DB_PASS = urllib.parse.quote_plus("Pankaj@123")
DB_HOST = "localhost"
DB_PORT = "3306"
DB_NAME = "mycrm_db"
db_url = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)

# ✅ Load data
df = pd.read_sql("SELECT * FROM email_data", engine)
df_countries = pd.read_sql("SELECT sortname, countries_name FROM tb_countries", engine)
df_countries['sortname'] = df_countries['sortname'].str.upper()

# ✅ Drop irrelevant columns
drop_cols = [
    'conversation_id', 'createdAt', 'from_name', 'from_email',
    'to_name', 'to_email', 'cc_name', 'cc_email',
    'subject_line', 'subject'
]
df.drop(columns=[col for col in drop_cols if col in df.columns], inplace=True)

# ✅ Clean column names and missing values
df.columns = df.columns.str.strip().str.replace(r'\s+', '', regex=True)
df.replace(['\\N', 'NaN', 'nan', '', 'None'], np.nan, inplace=True)

# ✅ Helper to extract numeric average
def extract_average(val):
    try:
        nums = [float(n) for n in re.findall(r'\d+\.?\d*', str(val))]
        return sum(nums) / len(nums) if nums else np.nan
    except:
        return np.nan

# ✅ Numeric columns
numeric_cols = ['n', 'ir', 'loi', 'requested_cpi', 'final_cpi', 'field_time']
for col in numeric_cols:
    df[col] = df[col].replace(r'[\$₹,%]', '', regex=True).apply(extract_average)

df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].median(numeric_only=True))
df = df[df['final_cpi'] >= 0]

# ✅ Replace market codes with country names
df['market'] = df['market'].str.upper()
df = df.merge(df_countries, left_on='market', right_on='sortname', how='left')
df.drop(columns=['market', 'sortname'], inplace=True)
df.rename(columns={'countries_name': 'market'}, inplace=True)

# ✅ Clean text columns
def clean_text(text):
    if pd.isna(text): return ''
    return re.sub(r'\s+', ' ', re.sub(r'[^a-zA-Z0-9 ]', ' ', str(text))).lower().strip()

text_cols = [
    'target_audience', 'industries', 'devices', 'client_name', 'methodology',
    'feasibility', 'company_size', 'languages', 'quotas',
    'departments', 'eligibility_criteria', 'survey_topic', 'survey_type'
]
for col in text_cols:
    if col in df.columns:
        df[col] = df[col].apply(clean_text)
    else:
        df[col] = ''

# ✅ Additional numeric
df['number_of_open_ends'] = df.get('number_of_open_ends', 0).apply(extract_average).fillna(0)

# ✅ IR/LOI score
df['efficiency_score'] = df['ir'] / df['loi']
df['efficiency_score'].replace([np.inf, -np.inf], np.nan, inplace=True)
df['efficiency_score'] = df['efficiency_score'].fillna(df['efficiency_score'].median())

# ✅ Full feature list
base_features = [
    'market', 'n', 'loi', 'ir', 'target_audience', 'industries',
    'methodology', 'feasibility', 'devices', 'field_time',
    'company_size', 'languages', 'quotas', 'departments',
    'number_of_open_ends', 'eligibility_criteria', 'survey_topic',
    'survey_type', 'efficiency_score'
]
target = 'final_cpi'

# ✅ Train model using Random Forest
def train_model(df, features, model_name_suffix):
    df_train = df[features + [target]].dropna()
    X = pd.get_dummies(df_train[features], dummy_na=True)
    X.columns = X.columns.str.replace(r'[<>\[\]]', '_', regex=True)
    y = np.log1p(df_train[target])
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    model = RandomForestRegressor(
        n_estimators=300,
        max_depth=15,
        random_state=42,
        n_jobs=-1
    )
    
    start = time.time()
    model.fit(X_train, y_train)
    print(f"✅ Model {model_name_suffix} trained in {round(time.time() - start, 2)}s")
    
    y_pred = np.expm1(model.predict(X_test))
    y_true = np.expm1(y_test)
    print(f"📊 {model_name_suffix} MSE: {mean_squared_error(y_true, y_pred):.2f}")
    print(f"📈 {model_name_suffix} R²: {r2_score(y_true, y_pred):.2f}")
    
    os.makedirs("ml", exist_ok=True)
    joblib.dump(model, f"ml/final_cpi_model_{model_name_suffix}.pkl")
    joblib.dump(X.columns.tolist(), f"ml/model_features_{model_name_suffix}.pkl")
    print(f"✅ {model_name_suffix} model and features saved.\n")

# 🔁 Train with requested_cpi
train_model(df.dropna(subset=['requested_cpi']), base_features + ['requested_cpi'], "with_cpi")

# 🔁 Train without requested_cpi (CPI halved)
df_without_cpi = df.copy()
df_without_cpi['final_cpi'] = df_without_cpi['final_cpi'] / 2
train_model(df_without_cpi, base_features, "without_cpi")
