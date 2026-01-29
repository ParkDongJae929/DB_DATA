import pandas as pd
from sqlalchemy import create_engine, inspect
import sys

# =============================================================================
# 1. 입력 DB 연결 설정 (원본 데이터 읽기용)
# =============================================================================
input_db_path = r'C:\Users\jay15\Desktop\DB_DATA\repo_trades_2025.db'
INPUT_CONN_STR = f"sqlite:///{input_db_path}"

try:
    # 원본 DB 엔진 생성
    input_engine = create_engine(INPUT_CONN_STR)
    input_conn = input_engine.connect()
    print(f"✅ 원본 DB 연결 성공: {input_db_path}")
    
    # 테이블 확인 및 선택
    inspector = inspect(input_engine)
    table_names = inspector.get_table_names()
    
    if 'repo_trades' in table_names:
        target_table_name = 'repo_trades'
    else:
        target_table_name = table_names[0]
        
    print(f"👉 분석 대상 테이블: '{target_table_name}'")

except Exception as e:
    print(f"❌ 원본 DB 연결 실패: {e}")
    sys.exit()

# =============================================================================
# 2. 데이터 추출 및 연산 (SQL에게 위임)
# =============================================================================
print("⏳ DB 엔진에서 가중평균 금리 계산 중... (메모리 최적화)")

# SQL에서 직접 가중평균(VWAP) 계산
query = f"""
    SELECT 
        basDt, 
        scrsItmsKcdNm,
        -- (금리 * 금액)의 합 / (금액)의 합 = 가중평균금리
        SUM( CAST(rpInrt AS REAL) * CAST(buyScrtBuyAmt AS REAL) ) / SUM( CAST(buyScrtBuyAmt AS REAL) ) as vwap_rate
    FROM 
        "{target_table_name}"
    WHERE 
        rpBuyAplCurCdNm = '대한민국 원'
        AND rdptTermCcdNm = '1영업일'
        AND basDt BETWEEN '20150101' AND '20251231'
        AND CAST(buyScrtBuyAmt AS REAL) > 0 
    GROUP BY 
        basDt, scrsItmsKcdNm
    ORDER BY 
        basDt
"""

try:
    df_result = pd.read_sql(query, input_conn)
    print(f"✅ 계산 완료! 요약 데이터 {len(df_result):,} 건 추출")
    
except Exception as e:
    print(f"❌ 쿼리 실행 실패: {e}")
    input_conn.close()
    sys.exit()

input_conn.close() # 원본 DB 연결 종료

# =============================================================================
# 3. 결과 정리 (Pivot)
# =============================================================================
if len(df_result) == 0:
    print("⚠️ 결과 데이터가 없습니다.")
    sys.exit()

# 날짜 형식 변환
df_result['basDt'] = pd.to_datetime(df_result['basDt'].astype(str))
df_result['vwap_rate'] = df_result['vwap_rate'].round(3)

# 피벗 (행: 날짜, 열: 담보종류, 값: 금리)
daily_repo_rates = df_result.pivot(index='basDt', columns='scrsItmsKcdNm', values='vwap_rate')
daily_repo_rates = daily_repo_rates.sort_index()

print("\n" + "="*50)
print("[미리보기] 산출된 레포 금리")
print("="*50)
print(daily_repo_rates.head())

# =============================================================================
# 4. 결과 저장 (SQLite DB 파일 생성)
# =============================================================================
# 저장할 새로운 DB 파일 경로
output_db_path = r'C:\Users\jay15\Desktop\DB_DATA\Daily_Repo_2025.db'
OUTPUT_CONN_STR = f"sqlite:///{output_db_path}"

# 저장할 테이블 이름
output_table_name = 'daily_repo_rates'

print(f"\n💾 결과 DB 저장 시작...")
print(f"   - 파일 경로: {output_db_path}")
print(f"   - 테이블명: {output_table_name}")

try:
    # 결과용 DB 엔진 생성
    output_engine = create_engine(OUTPUT_CONN_STR)
    
    # DB에 테이블로 저장 (if_exists='replace': 기존 파일 있으면 덮어쓰기)
    # index=True 옵션으로 'basDt' 날짜 컬럼도 DB에 같이 저장됩니다.
    daily_repo_rates.to_sql(output_table_name, output_engine, if_exists='replace', index=True)
    
    print("✅ DB 저장 완료! (성공)")

except Exception as e:
    print(f"❌ 저장 실패: {e}")