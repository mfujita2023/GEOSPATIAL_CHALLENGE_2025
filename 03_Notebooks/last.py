import pandas as pd
import numpy as np
import lightgbm as lgb
from snowflake.snowpark.context import get_active_session
from sklearn.model_selection import KFold
from sklearn.preprocessing import LabelEncoder

session = get_active_session()
df = session.table("GEO_CHALLENGE.DBT.FCT_ML_READY").to_pandas()
df.columns = [c.upper() for c in df.columns]

# --- 1. 高精度補完（FULL_ADDRESS活用） ---
df['BUILDING_AGE'] = df.groupby('FULL_ADDRESS')['BUILDING_AGE'].transform(lambda x: x.fillna(x.median()))
df['BUILDING_AGE'] = df['BUILDING_AGE'].fillna(df['BUILDING_AGE'].median())
df['HOUSE_AREA'] = df.groupby('ADDR_GROUP')['HOUSE_AREA'].transform(lambda x: x.fillna(x.median()))
df['HOUSE_AREA'] = df['HOUSE_AREA'].fillna(df['HOUSE_AREA'].median())

# --- 2. 交互作用特徴量（ここで解像度を上げる） ---
# 面積 × 築年数：広いけど古い、狭いけど新しい、の差を強調
df['AREA_AGE_INTER'] = df['HOUSE_AREA'] * (df['BUILDING_AGE'] + 1)
# 面積 × 地価：土地としての資産価値
df['LAND_VALUE_EST'] = df['HOUSE_AREA'] * df['NEAREST_LAND_PRICE']
# 徒歩距離の逆数
df['INV_WALK_DIST'] = 1.0 / (df['FINAL_WALK_DIST'] + 1.0)

# --- 3. カテゴリID化 ---
cat_cols = ['STATION_NAME', 'CITY_NAME', 'ADDR_GROUP']
for col in cat_cols:
    df[col+'_ID'] = LabelEncoder().fit_transform(df[col].astype(str))

# --- 4. OOF Target Encoding (4-Foldでバランス調整) ---
train_idx = df[df['DATA_TYPE'] == 'train'].index
test_idx = df[df['DATA_TYPE'] == 'test'].index
df.loc[train_idx, 'UP_TEMP'] = df.loc[train_idx, 'TARGET_PRICE'] / df.loc[train_idx, 'HOUSE_AREA']

# 💡 median（中央値）に戻して堅牢性を高める
kf_te = KFold(n_splits=4, shuffle=True, random_state=42)
df['BLDG_TE'] = np.nan
df['ADDR_TE'] = np.nan

for tr_f, val_f in kf_te.split(train_idx):
    tmp_tr = df.iloc[train_idx[tr_f]]
    b_map = tmp_tr.groupby('BUILDING_ID')['TARGET_PRICE'].median()
    a_map = tmp_tr.groupby('ADDR_GROUP')['UP_TEMP'].median()
    df.loc[train_idx[val_f], 'BLDG_TE'] = df.loc[train_idx[val_f], 'BUILDING_ID'].map(b_map)
    df.loc[train_idx[val_f], 'ADDR_TE'] = df.loc[train_idx[val_f], 'ADDR_GROUP'].map(a_map)

full_tr = df.loc[train_idx]
df.loc[test_idx, 'BLDG_TE'] = df.loc[test_idx, 'BUILDING_ID'].map(full_tr.groupby('BUILDING_ID')['TARGET_PRICE'].median())
df.loc[test_idx, 'ADDR_TE'] = df.loc[test_idx, 'ADDR_GROUP'].map(full_tr.groupby('ADDR_GROUP')['UP_TEMP'].median())

# 欠損補完（市区町村中央値）
city_map = full_tr.groupby('CITY_NAME')['TARGET_PRICE'].median()
df['BLDG_TE'] = df['BLDG_TE'].fillna(df['CITY_NAME'].map(city_map)).fillna(full_tr['TARGET_PRICE'].median())
df['ADDR_TE'] = df['ADDR_TE'].fillna(full_tr['UP_TEMP'].median())

# --- 5. 学習実行 (4-Fold / 15分完走設定) ---
features = [
    'HOUSE_AREA', 'FLOOR_COUNT', 'BUILDING_AGE', 'LAT', 'LON', 
    'FINAL_WALK_DIST', 'INV_WALK_DIST', 'NEAREST_LAND_PRICE', 'LAND_GROWTH_RATE',
    'AREA_AGE_INTER', 'LAND_VALUE_EST', 'BLDG_TE', 'ADDR_TE',
    'STATION_NAME_ID', 'CITY_NAME_ID', 'ADDR_GROUP_ID'
]
X = df.loc[train_idx, features]
y_log = np.log1p(df.loc[train_idx, 'TARGET_PRICE'])
X_test = df.loc[test_idx, features]
cat_features = [f for f in features if f.endswith('_ID')]

test_preds = np.zeros(len(X_test))
lgb_params = {
    'objective': 'regression', 'metric': 'mae', 'learning_rate': 0.03,
    'num_leaves': 255, 'feature_fraction': 0.7, 'random_state': 42, 'verbosity': -1
}

kf_train = KFold(n_splits=4, shuffle=True, random_state=42)
for fold, (tr, val) in enumerate(kf_train.split(X, y_log)):
    t_set = lgb.Dataset(X.iloc[tr], label=y_log.iloc[tr], categorical_feature=cat_features)
    v_set = lgb.Dataset(X.iloc[val], label=y_log.iloc[val], categorical_feature=cat_features, reference=t_set)
    
    model = lgb.train(lgb_params, t_set, valid_sets=[v_set], 
                      num_boost_round=3000, callbacks=[lgb.early_stopping(100)])
    test_preds += np.expm1(model.predict(X_test)) / 4
    print(f"Fold {fold+1} 完了")

# --- 6. 提出 ---
test_df = df.loc[test_idx].copy()
test_df['PRICE'] = np.round(test_preds).astype(int)
test_df[['ID', 'PRICE']].to_csv("submission_final_14.csv", header=False, index=False)
session.file.put("submission_final_14.csv", "@SUBMIT_STAGE", overwrite=True, auto_compress=False)
print("【最終ミッション】完了！14点台への扉は開きました。")