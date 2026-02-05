import pandas as pd
import sqlite3
from sqlalchemy import create_engine

# =============================================================================
# 설정
# =============================================================================
BASE_PATH = r'C:\Users\jay15\Desktop\DB_DATA\DataBase'

# 입력 DB 파일들
input_dbs = [
    f'{BASE_PATH}\\D_Repo_2015-2019.db',
    f'{BASE_PATH}\\D_Repo_2020-2024.db',
    f'{BASE_PATH}\\D_Repo_2025.db'
]

# 출력 DB 파일
output_db = f'{BASE_PATH}\\D_Repo_2015-2025.db'

# =============================================================================
# DB 통합
# =============================================================================
print("=" * 60)
print("📂 DB 통합 시작")
print("=" * 60)

df_list = []

for db_path in input_dbs:
    try:
        conn = sqlite3.connect(db_path)
        df_temp = pd.read_sql("SELECT * FROM daily_repo_rates", conn)
        conn.close()
        
        # 날짜 컬럼 처리
        if 'basDt' in df_temp.columns:
            df_temp['date'] = pd.to_datetime(df_temp['basDt'])
            df_temp = df_temp.drop(columns=['basDt'])
        elif 'index' in df_temp.columns:
            df_temp['date'] = pd.to_datetime(df_temp['index'])
            df_temp = df_temp.drop(columns=['index'])
        
        df_list.append(df_temp)
        print(f"  ✓ {db_path.split(chr(92))[-1]}: {len(df_temp)}일")
        
    except Exception as e:
        print(f"  ✗ {db_path}: 로드 실패 ({e})")

# 통합
df_combined = pd.concat(df_list, ignore_index=True)
df_combined = df_combined.drop_duplicates(subset=['date'], keep='first')
df_combined = df_combined.sort_values('date').reset_index(drop=True)
df_combined = df_combined.set_index('date')

print(f"\n  → 통합 완료: {len(df_combined)}일")
print(f"  → 기간: {df_combined.index.min().strftime('%Y-%m-%d')} ~ {df_combined.index.max().strftime('%Y-%m-%d')}")
print(f"  → 컬럼: {df_combined.columns.tolist()}")

# =============================================================================
# 저장
# =============================================================================
print(f"\n💾 저장 중...")

engine = create_engine(f"sqlite:///{output_db}")
df_combined.to_sql('daily_repo_rates', engine, if_exists='replace', index=True, index_label='basDt')

print(f"  ✓ 저장 완료: {output_db}")

# =============================================================================
# 확인
# =============================================================================
print(f"\n{'='*60}")
print("📊 저장 결과 확인")
print("=" * 60)

conn_check = sqlite3.connect(output_db)
df_check = pd.read_sql("SELECT * FROM daily_repo_rates LIMIT 5", conn_check)
conn_check.close()

print(df_check)
print(f"\n✅ 통합 완료!")