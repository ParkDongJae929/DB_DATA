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

# -----------------------------------------------------------------------------
# 2-1) 담보별 가중평균 금리
# -----------------------------------------------------------------------------
query_by_collateral = f"""
    SELECT 
        basDt, 
        scrsItmsKcdNm,
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

# -----------------------------------------------------------------------------
# 2-2) 전체 가중평균 금리 (담보 구분 없이)
# -----------------------------------------------------------------------------
query_total = f"""
    SELECT 
        basDt, 
        '전체' as scrsItmsKcdNm,
        SUM( CAST(rpInrt AS REAL) * CAST(buyScrtBuyAmt AS REAL) ) / SUM( CAST(buyScrtBuyAmt AS REAL) ) as vwap_rate
    FROM 
        "{target_table_name}"
    WHERE 
        rpBuyAplCurCdNm = '대한민국 원'
        AND rdptTermCcdNm = '1영업일'
        AND basDt BETWEEN '20150101' AND '20251231'
        AND CAST(buyScrtBuyAmt AS REAL) > 0 
    GROUP BY 
        basDt
    ORDER BY 
        basDt
"""

try:
    # 담보별 쿼리 실행
    df_by_collateral = pd.read_sql(query_by_collateral, input_conn)
    print(f"✅ 담보별 계산 완료! {len(df_by_collateral):,} 건")
    
    # 전체 쿼리 실행
    df_total = pd.read_sql(query_total, input_conn)
    print(f"✅ 전체 계산 완료! {len(df_total):,} 건")
    
    # 두 결과 합치기
    df_result = pd.concat([df_by_collateral, df_total], ignore_index=True)
    print(f"✅ 통합 완료! 총 {len(df_result):,} 건")
    
except Exception as e:
    print(f"❌ 쿼리 실행 실패: {e}")
    input_conn.close()
    sys.exit()

input_conn.close()  # 원본 DB 연결 종료

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

# 컬럼 순서 정리 ('전체'를 맨 앞으로)
cols = daily_repo_rates.columns.tolist()
if '전체' in cols:
    cols.remove('전체')
    cols = ['전체'] + sorted(cols)
    daily_repo_rates = daily_repo_rates[cols]

print("\n" + "=" * 60)
print("[미리보기] 산출된 레포 금리 (담보별 + 전체)")
print("=" * 60)
print(daily_repo_rates.head(10))

print(f"\n[컬럼 목록]")
print(f"  {daily_repo_rates.columns.tolist()}")

print(f"\n[기간]")
print(f"  시작: {daily_repo_rates.index.min()}")
print(f"  종료: {daily_repo_rates.index.max()}")
print(f"  일수: {len(daily_repo_rates)}일")

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
    daily_repo_rates.to_sql(output_table_name, output_engine, if_exists='replace', index=True)
    
    print("✅ DB 저장 완료! (성공)")

except Exception as e:
    print(f"❌ 저장 실패: {e}")

# =============================================================================
# 5. 저장 결과 확인
# =============================================================================
print(f"\n{'='*60}")
print("📊 저장 결과 요약")
print("=" * 60)
print(f"  - 총 일수: {len(daily_repo_rates)}일")
print(f"  - 담보유형: {len(daily_repo_rates.columns)}개")
print(f"  - 컬럼: {daily_repo_rates.columns.tolist()}")