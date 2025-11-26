# pyspark-apps programs
# 서울 총조사 주요지표 파이프라인 (SGIS)

## 개요
- 통계지리정보서비스(SGIS) "총조사 주요지표"를 수집해 원시/가공 데이터를 S3에 적재하고, Lambda로 변환 후 Glue/Athena/Redshift/QuickSight로 분석까지 이어지는 소규모 프로젝트입니다.
- 전체 흐름은 12.png 아키텍처와 유사하게 EC2(또는 로컬) → S3 Raw → Lambda 변환 → S3 Processed → Glue Data Catalog → Athena/Redshift/QuickSight 로 이어집니다.

## 코드 구성
- `Main.py`: SGIS API 토큰 발급 → 인구 통계 수집 → 로컬 저장 → S3 업로드. CLI 인자 `-i y|n`, `-a <adm_cd>`, `-y <year>` 지원.
- `lambda_function.py`: S3 생성 이벤트로 트리거되어 Raw JSON Lines를 읽고 컬럼 보강/타입 변환 후 `total_statistics_processed/` 경로로 저장.
- `test.py`: 액세스 토큰, population API, S3 업로드를 순차로 점검하는 로컬 테스트 스크립트.
- `.env`: SGIS 및 S3 자격증명 환경변수(`SGIS_CONSUMER_KEY`, `SGIS_CONSUMER_SECRET`, `BUCKET_NAME`).

## 실행/수집 방법 (`Main.py`)
```bash
# 초기 전체 수집 (2019~2023, 서울 25개 구)
python Main.py -i y

# 특정 구/연도만 수집
a: 행정구역 코드(5자리), y: 기준연도
python Main.py -i n -a 11010 -y 2023
```
- 로컬 저장: `total_statistics_raw/year=<YYYY>/adm_cd=<code>/result.json` (JSON Lines)
- S3 저장:   `s3://<BUCKET_NAME>/total_statistics_raw/<YYYY>/adm_cd=<code>/result.json`

## Lambda 변환 (`lambda_function.py`)
- 트리거: S3 "모든 객체 생성" 이벤트 (`total_statistics_raw/` 하위).
- 처리:
  - 이벤트 key에서 `year`, `adm_cd` 추출.
  - 서울 구 코드 매핑으로 `upper_adm_cd`, `upper_adm_nm` 추가.
  - `N/A`/빈 값은 `None` 처리 후 숫자/실수/문자 타입으로 캐스팅.
- 결과 저장: `s3://<BUCKET_NAME>/total_statistics_processed/total_year=<YYYY>/total_adm_cd=<code>/result.json` (JSON Lines)

## 데이터 스키마 (가공본 기준)
- 파티션: `total_year`(문자), `total_adm_cd`(문자)
- 컬럼: year(int), adm_cd(str), adm_nm(str), upper_adm_cd(str), upper_adm_nm(str), tot_ppltn(int), avg_age(float), ppltn_dnsty(float), aged_child_idx(float), oldage_suprt_per(float), juv_suprt_per(float), tot_family(int), avg_fmember_cnt(float), tot_house(int), nongga_cnt(int), nongga_ppltn(int), imga_cnt(int), imga_ppltn(int), naesuoga_cnt(int), naesuoga_ppltn(int), haesuoga_cnt(int), haesuoga_ppltn(int), employee_cnt(int), corp_cnt(int)

## Glue / Athena / Redshift / QuickSight 가이드
- Glue 테이블 예시: `population_total_statistics`, 파티션 `total_year`, `total_adm_cd`; 컬럼 타입은 위 스키마 사용.
- Athena 예시 쿼리:
  - 2023년 평균 연령 상위 5개 구: `SELECT adm_nm, avg_age FROM population_total_statistics WHERE total_year='2023' ORDER BY avg_age DESC LIMIT 5;`
  - 2022년 사업체당 임직원 상위 10개: `SELECT adm_nm, SUM(employee_cnt) AS employees, SUM(corp_cnt) AS corps, ROUND(SUM(employee_cnt)/NULLIF(SUM(corp_cnt),0),1) AS employees_per_corp FROM population_total_statistics WHERE total_year='2022' GROUP BY adm_nm ORDER BY employees_per_corp DESC LIMIT 10;`
- Redshift 적재: dc2.large 클러스터 생성 후 동일 스키마로 `population_total_statistics` 테이블 생성, `COPY` 명령으로 `s3://<BUCKET_NAME>/total_statistics_processed/` 적재.
- QuickSight: 2023년 서울 데이터로 평균 연령/인구/사업체 등 시각화, 연도별(2015~2023) 추세, 2022년 구별 기업 수·직원 수 차트 구성.

## 필요 패키지
- `requests`, `jsonlines`, `boto3`, `python-dotenv`

## 참고
- 아키텍처 다이어그램: `12.png`
- 과제 요구사항 상세: ` work.docx`
