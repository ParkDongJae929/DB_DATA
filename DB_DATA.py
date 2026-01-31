"""
금융위원회 REPO거래정보 - 건별거래조회 (SQLite 버전)
데이터를 SQLite DB에 저장하고 관리
"""

import sys
import io
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import sqlite3

# Windows 콘솔 인코딩 문제 해결
if sys.platform == 'win32' and hasattr(sys.stdout, 'buffer'):
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
    except:
        pass

# =============================================================================
# 📅 날짜 설정 - 여기를 수정하세요!
# =============================================================================
START_DATE = '20250101'  # 시작 날짜 (YYYYMMDD 형식)
END_DATE = '20251231'    # 종료 날짜 (YYYYMMDD 형식)
# =============================================================================

# 데이터베이스 파일명
DB_FILE = 'repo_trades_2025.db'

# API 설정
BASE_URL = 'http://apis.data.go.kr/1160100/service/GetRepoTradInfoService/getCaseForTrad'
SERVICE_KEY = '8e2d2fb441c63432251207ba4c64e26e90b7939e40980fdcff287553c5867f9a'

def init_database():
    """
    데이터베이스 및 테이블 초기화
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # 거래 데이터 테이블 생성
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS repo_trades (
            basDt TEXT,
            rpSqno TEXT,
            rpBuyAplCurCd TEXT,
            rpBuyAplCurCdNm TEXT,
            rdptTermCcd TEXT,
            rdptTermCcdNm TEXT,
            rpRmngExprDcd TEXT,
            rpRmngExprDcdNm TEXT,
            rpInrt REAL,
            slngShtrFinBzcDcd TEXT,
            slngShtrFinBzcDcdNm TEXT,
            buynShtrFinBzcDcd TEXT,
            buynShtrFinBzcDcdNm TEXT,
            rpOpngDt TEXT,
            rpBuyAmt REAL,
            rpMrgamRto REAL,
            scrsItmsKcd TEXT,
            scrsItmsKcdNm TEXT,
            isinCd TEXT,
            isinCdNm TEXT,
            buyScrtBuyAmt REAL,
            buyScrtEvlAmt REAL,
            PRIMARY KEY (basDt, rpSqno)
        )
    ''')
    
    # 수집 상태 추적 테이블 생성
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS collection_status (
            basDt TEXT PRIMARY KEY,
            total_count INTEGER,
            collected_count INTEGER,
            collected_at TEXT,
            status TEXT
        )
    ''')
    
    # 인덱스 생성 (조회 속도 향상)
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_basDt ON repo_trades(basDt)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_slng ON repo_trades(slngShtrFinBzcDcdNm)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_buyn ON repo_trades(buynShtrFinBzcDcdNm)')
    
    conn.commit()
    conn.close()
    
    print(f"✓ 데이터베이스 초기화 완료: {DB_FILE}")

def is_date_collected(base_date):
    """
    해당 날짜가 이미 수집되었는지 확인
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT status FROM collection_status 
        WHERE basDt = ? AND status = 'completed'
    ''', (base_date,))
    
    result = cursor.fetchone()
    conn.close()
    
    return result is not None

def get_repo_trades(base_date, num_rows=100, page_no=1, retry=3):
    """API 호출 (재시도 로직 포함)"""
    params = {
        'serviceKey': SERVICE_KEY,
        'numOfRows': str(num_rows),
        'pageNo': str(page_no),
        'resultType': 'json',
        'basDt': base_date
    }
    
    for attempt in range(retry):
        try:
            response = requests.get(BASE_URL, params=params, timeout=60)
            
            if response.status_code == 200:
                data = response.json()
                if 'response' in data:
                    header = data['response'].get('header', {})
                    if header.get('resultCode') == '00':
                        return data
        except requests.exceptions.Timeout:
            if attempt < retry - 1:
                print(f" (타임아웃, {attempt+1}/{retry} 재시도)", end="")
                time.sleep(2)
                continue
            else:
                print(f" (타임아웃 실패)")
                return None
        except Exception as e:
            if attempt < retry - 1:
                print(f" (오류, {attempt+1}/{retry} 재시도)", end="")
                time.sleep(2)
                continue
            else:
                print(f" (오류: {e})")
                return None
    
    return None

def save_trades_to_db(trades_data, base_date):
    """
    거래 데이터를 DB에 저장
    """
    if not trades_data:
        return 0
    
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    saved_count = 0
    for trade in trades_data:
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO repo_trades (
                    basDt, rpSqno, rpBuyAplCurCd, rpBuyAplCurCdNm,
                    rdptTermCcd, rdptTermCcdNm, rpRmngExprDcd, rpRmngExprDcdNm,
                    rpInrt, slngShtrFinBzcDcd, slngShtrFinBzcDcdNm,
                    buynShtrFinBzcDcd, buynShtrFinBzcDcdNm, rpOpngDt,
                    rpBuyAmt, rpMrgamRto, scrsItmsKcd, scrsItmsKcdNm,
                    isinCd, isinCdNm, buyScrtBuyAmt, buyScrtEvlAmt
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                trade.get('basDt'), trade.get('rpSqno'),
                trade.get('rpBuyAplCurCd'), trade.get('rpBuyAplCurCdNm'),
                trade.get('rdptTermCcd'), trade.get('rdptTermCcdNm'),
                trade.get('rpRmngExprDcd'), trade.get('rpRmngExprDcdNm'),
                trade.get('rpInrt'), trade.get('slngShtrFinBzcDcd'),
                trade.get('slngShtrFinBzcDcdNm'), trade.get('buynShtrFinBzcDcd'),
                trade.get('buynShtrFinBzcDcdNm'), trade.get('rpOpngDt'),
                trade.get('rpBuyAmt'), trade.get('rpMrgamRto'),
                trade.get('scrsItmsKcd'), trade.get('scrsItmsKcdNm'),
                trade.get('isinCd'), trade.get('isinCdNm'),
                trade.get('buyScrtBuyAmt'), trade.get('buyScrtEvlAmt')
            ))
            saved_count += 1
        except sqlite3.IntegrityError:
            # 중복 데이터는 무시
            continue
    
    conn.commit()
    conn.close()
    
    return saved_count

def update_collection_status(base_date, total_count, collected_count, status='completed'):
    """
    수집 상태 업데이트
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT OR REPLACE INTO collection_status 
        (basDt, total_count, collected_count, collected_at, status)
        VALUES (?, ?, ?, ?, ?)
    ''', (base_date, total_count, collected_count, datetime.now().isoformat(), status))
    
    conn.commit()
    conn.close()

def collect_date_data(base_date):
    """
    특정 날짜의 모든 데이터 수집
    """
    # 이미 수집된 날짜인지 확인
    if is_date_collected(base_date):
        print(f"{base_date}: 이미 수집 완료 (건너뛰기)")
        return True
    
    print(f"\n{base_date} 데이터 조회 중...", end=" ")
    
    # 첫 페이지 조회
    result = get_repo_trades(base_date, num_rows=1000, page_no=1)
    
    if not result or 'response' not in result:
        print("조회 실패")
        return False
    
    body = result['response'].get('body', {})
    total_count = body.get('totalCount', 0)
    
    if total_count == 0:
        print("데이터 없음")
        update_collection_status(base_date, 0, 0, 'no_data')
        return True
    
    # 첫 페이지 데이터 저장
    items = body.get('items', {}).get('item', [])
    if not isinstance(items, list):
        items = [items]
    
    saved = save_trades_to_db(items, base_date)
    print(f"OK - {saved}건 수집 (전체 {total_count}건)", end="")
    
    total_saved = saved
    
    # 나머지 페이지 수집
    if total_count > 1000:
        pages = (total_count // 1000) + 1
        for page in range(2, pages + 1):
            result_page = get_repo_trades(base_date, num_rows=1000, page_no=page)
            if result_page and 'response' in result_page:
                items_page = result_page['response'].get('body', {}).get('items', {}).get('item', [])
                if not isinstance(items_page, list):
                    items_page = [items_page]
                saved_page = save_trades_to_db(items_page, base_date)
                total_saved += saved_page
                
                # 진행률 표시
                if page % 5 == 0:
                    print(f"\n  → {page}/{pages}페이지 진행 중 ({total_saved}건 저장)", end="")
            
            time.sleep(0.5)  # API 제한 준수
    
    print(f"\n  ✓ 완료: {total_saved}건 저장됨")
    
    # 수집 완료 상태 저장
    update_collection_status(base_date, total_count, total_saved, 'completed')
    
    return True

def collect_date_range(start_date, end_date):
    """
    날짜 범위의 데이터 수집 (주말 제외)
    """
    start_dt = datetime.strptime(start_date, '%Y%m%d')
    end_dt = datetime.strptime(end_date, '%Y%m%d')
    
    # 전체 날짜 수와 평일 날짜 수 계산
    total_days = (end_dt - start_dt).days + 1
    weekday_count = 0
    temp_date = start_dt
    while temp_date <= end_dt:
        if temp_date.weekday() < 5:  # 0=월요일, 4=금요일
            weekday_count += 1
        temp_date += timedelta(days=1)
    
    print(f"\n{'='*80}")
    print(f"데이터 수집 기간: {start_dt.strftime('%Y-%m-%d')} ~ {end_dt.strftime('%Y-%m-%d')}")
    print(f"총 {total_days}일 (평일 {weekday_count}일, 주말 {total_days - weekday_count}일)")
    print(f"{'='*80}")
    
    current_date = start_dt
    success_count = 0
    fail_count = 0
    skipped_weekend = 0
    
    while current_date <= end_dt:
        # 주말 체크 (0=월요일, 5=토요일, 6=일요일)
        if current_date.weekday() >= 5:
            date_str = current_date.strftime('%Y%m%d')
            weekday_name = '토요일' if current_date.weekday() == 5 else '일요일'
            print(f"{date_str} ({weekday_name}): 주말 - 건너뛰기")
            skipped_weekend += 1
            current_date += timedelta(days=1)
            continue
        
        date_str = current_date.strftime('%Y%m%d')
        
        if collect_date_data(date_str):
            success_count += 1
        else:
            fail_count += 1
        
        current_date += timedelta(days=1)
        time.sleep(0.5)  # 날짜 간 대기
    
    print(f"\n{'='*80}")
    print(f"수집 완료 - 평일 {success_count}일 수집, 실패: {fail_count}일, 주말 제외: {skipped_weekend}일")
    print(f"{'='*80}\n")

def get_db_stats():
    """
    데이터베이스 통계 조회
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # 전체 거래 건수
    cursor.execute('SELECT COUNT(*) FROM repo_trades')
    total_trades = cursor.fetchone()[0]
    
    # 날짜 범위
    cursor.execute('SELECT MIN(basDt), MAX(basDt) FROM repo_trades')
    date_range = cursor.fetchone()
    
    # 수집 완료된 날짜 수
    cursor.execute("SELECT COUNT(*) FROM collection_status WHERE status='completed'")
    completed_dates = cursor.fetchone()[0]
    
    conn.close()
    
    print(f"\n{'='*80}")
    print(f"데이터베이스 통계")
    print(f"{'='*80}")
    print(f"DB 파일: {DB_FILE}")
    print(f"총 거래 건수: {total_trades:,}건")
    if date_range[0]:
        print(f"데이터 기간: {date_range[0]} ~ {date_range[1]}")
    print(f"수집 완료 날짜: {completed_dates}일")
    print(f"{'='*80}\n")

def export_to_excel(output_file='repo_trades_export.xlsx', start_date=None, end_date=None):
    """
    DB 데이터를 엑셀로 내보내기
    """
    conn = sqlite3.connect(DB_FILE)
    
    if start_date and end_date:
        query = f"SELECT * FROM repo_trades WHERE basDt BETWEEN '{start_date}' AND '{end_date}' ORDER BY basDt, rpSqno"
        print(f"기간 {start_date} ~ {end_date} 데이터를 내보냅니다...")
    else:
        query = "SELECT * FROM repo_trades ORDER BY basDt, rpSqno"
        print("전체 데이터를 내보냅니다...")
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        print("내보낼 데이터가 없습니다.")
        return
    
    df.to_excel(output_file, index=False, engine='openpyxl')
    print(f"✓ {len(df):,}건의 데이터를 '{output_file}'에 저장했습니다.")

def query_data(sql_query):
    """
    사용자 정의 SQL 쿼리 실행
    """
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query(sql_query, conn)
    conn.close()
    return df

def test_api_connection():
    """API 연결 테스트"""
    print("API 연결 테스트 중...")
    print("=" * 80)
    
    test_date = "20241220"
    result = get_repo_trades(test_date, num_rows=5, page_no=1)
    
    if result:
        print(f"✓ API 연결 성공!")
        body = result['response'].get('body', {})
        print(f"테스트 날짜 {test_date}: {body.get('totalCount', 0)}건 조회 가능")
        return True
    else:
        print("✗ API 연결 실패")
        return False

def main():
    """
    메인 실행 함수
    """
    print("=" * 80)
    print("금융위원회 REPO거래정보 - SQLite 버전")
    print("=" * 80)
    print()
    
    # API 연결 테스트
    if not test_api_connection():
        print("\nAPI 연결에 실패했습니다.")
        return
    
    print()
    
    # 데이터베이스 초기화
    init_database()
    print()
    
    # 데이터 수집
    collect_date_range(START_DATE, END_DATE)
    
    # 통계 출력
    get_db_stats()
    
    print("\n프로그램 완료!")

if __name__ == "__main__":
    main()