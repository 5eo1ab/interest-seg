------------------------------------------
-- Target Master 데이터 생성 
-- 테이블명: 	SBXL2CK.L2CK_HB5EO_REFINED_TARGET_TEMP
-- 샘플추출 테이블: 		SBXL2CK.L2CK_HB5EO_SAMPLED_TARGET_TEMP
-- Count: 		3,966,698 건  ->		70,525 건
-- 	
------------------------------------------

--SET MEM_LIMIT= '7g';
--SET BATCH_SIZE = 10;


------------------------------
-- 기본 명세 정보
------------------------------
TRUNCATE TABLE IF EXISTS SBXL2CK.L2CK_HB5EO_BASE_TARGET_TEMP;
DROP TABLE IF EXISTS SBXL2CK.L2CK_HB5EO_BASE_TARGET_TEMP;
CREATE TABLE SBXL2CK.L2CK_HB5EO_BASE_TARGET_TEMP 
	STORED AS PARQUET
	LOCATION '/SBX/impala_table/L2/CK/L2CK_HB5EO_BASE_TARGET_TEMP' AS 
SELECT MEMT.entr_no, MEMT.cust_no, MEMT.PRD_NO AS imsi_no
, MEMT.sex_dv_cd
, CAST(MEMT.cust_age/10 AS INT) AS cust_age_idx
, HSE.hse_id, HSE.hse_sgmt_cd, HSE.hse_sgmt_nm
, LOC.rsdc_xcrd_addr, LOC.rsdc_ycrd_addr
-- , CII.offc_wkr_yn
FROM BDPVIEW.L1AAT_MBL_ENTR_MNTH_TXN AS MEMT
JOIN BDPVIEW.L2EST_CUST_WKDY_PMAC_LOC AS LOC ON LOC.imsi = MEMT.PRD_NO
JOIN BDPVIEW.L2EWT_HSE_IST AS HSE ON HSE.entr_no = MEMT.entr_no
-- JOIN BDPVIEW.L2EOT_CUST_IDVL_IDEX_M AS CII ON CII.entr_no = MEMT.entr_no
WHERE 1=1
-- 기준년월: 2022년 1월
AND MEMT.base_yymm = '202201'
AND LOC.p_basis_yyyy = 2022 AND LOC.p_basis_mm = 1
AND HSE.p_basis_yyyy = 2022 AND HSE.p_basis_mm = 1
-- AND CII.p_basis_yyyy = 2022 AND CII.p_basis_mm = 1
-- 가입상태 조건: 당월에 정지없이 전 기간을 사용
-- 해지 제외
AND MEMT.THMN_CTNU_USE_YN = 1
AND MEMT.ENTR_STTS_CD != 'C'
AND MEMT.sex_dv_cd IS NOT NULL
-- 고객 연령대: 20세 이상 59세 이하
AND CAST(MEMT.cust_age AS INT) BETWEEN 20 AND 49
-- 가구Segment코드 NOT NULL
AND HSE.hse_sgmt_cd IS NOT NULL
-- 직장인여부 = True
-- AND CII.offc_wkr_yn = 1
;



------------------------------
-- 주중 대표일(13일 수요일) 야간 코어타임(2시~3시) 좌표 구간
------------------------------
TRUNCATE TABLE IF EXISTS SBXL2CK.L2CK_HB5EO_BASE_NIGHTTIME_ACTIVE_RANGE_TEMP;
DROP TABLE IF EXISTS SBXL2CK.L2CK_HB5EO_BASE_NIGHTTIME_ACTIVE_RANGE_TEMP;
CREATE TABLE SBXL2CK.L2CK_HB5EO_BASE_NIGHTTIME_ACTIVE_RANGE_TEMP 
	STORED AS PARQUET
	LOCATION '/SBX/impala_table/L2/CK/L2CK_HB5EO_BASE_NIGHTTIME_ACTIVE_RANGE_TEMP' AS 
SELECT TCCM.imsi_no
, MIN(TCCM.cell_xcrd) as cell_xcrd_min
, MAX(TCCM.cell_xcrd) as cell_xcrd_max
, MIN(TCCM.cell_ycrd) as cell_ycrd_min
, MAX(TCCM.cell_ycrd) as cell_ycrd_min
FROM BDPVIEW.L1DA_TB_CEBY_CUST_MVMT_H AS TCCM
WHERE 1=1
AND TCCM.p_basis_yyyy = 2022 
AND TCCM.p_basis_mm = 1
AND TCCM.p_basis_dd = 12
AND FROM_TIMESTAMP(TCCM.mt10_unit_tm,'HH') = '02'
GROUP BY 1
;



------------------------------
-- 기준좌표(추정거주지) 및 야간 코어타임(2시~3시) 좌표 표본표준편차(sttd)
------------------------------
TRUNCATE TABLE IF EXISTS SBXL2CK.L2CK_HB5EO_BASE_RSDC_SSTD_TEMP;
DROP TABLE IF EXISTS SBXL2CK.L2CK_HB5EO_BASE_RSDC_SSTD_TEMP;
CREATE TABLE SBXL2CK.L2CK_HB5EO_BASE_RSDC_SSTD_TEMP 
	STORED AS PARQUET
	LOCATION '/SBX/impala_table/L2/CK/L2CK_HB5EO_BASE_RSDC_SSTD_TEMP' AS 
SELECT R.imsi_no
, T.rsdc_xcrd_addr, T.rsdc_ycrd_addr
, SQRT((
	POW(T.rsdc_xcrd_addr-R.cell_xcrd_min,2)+POW(T.rsdc_xcrd_addr-R.cell_xcrd_max,2)
)/2) AS sstd_xcrd
, SQRT((
	POW(T.rsdc_ycrd_addr-R.cell_ycrd_min,2)+POW(T.rsdc_ycrd_addr-R.cell_ycrd_max,2)
)/2) AS sstd_ycrd
FROM SBXL2CK.L2CK_HB5EO_BASE_NIGHTTIME_ACTIVE_RANGE_TEMP AS R
JOIN SBXL2CK.L2CK_HB5EO_BASE_TARGET_TEMP AS T ON T.imsi_no = R.imsi_no
WHERE 1=1
;


------------------------------
-- 주중 5일(10일~14일)간 주간 코어타임(13시~14시) 좌표 구간
------------------------------
TRUNCATE TABLE IF EXISTS SBXL2CK.L2CK_HB5EO_DLY_DAYTIME_ACTIVE_RANGE_TEMP;
DROP TABLE IF EXISTS SBXL2CK.L2CK_HB5EO_DLY_DAYTIME_ACTIVE_RANGE_TEMP;
CREATE TABLE SBXL2CK.L2CK_HB5EO_DLY_DAYTIME_ACTIVE_RANGE_TEMP 
	STORED AS PARQUET
	LOCATION '/SBX/impala_table/L2/CK/L2CK_HB5EO_DLY_DAYTIME_ACTIVE_RANGE_TEMP' AS 
SELECT TCCM.imsi_no, FROM_TIMESTAMP(TCCM.mt10_unit_tm,'yyyyMMdd') as yyyymmdd
, MIN(TCCM.cell_xcrd) as cell_xcrd_min
, MAX(TCCM.cell_xcrd) as cell_xcrd_max
, MIN(TCCM.cell_ycrd) as cell_ycrd_min
, MAX(TCCM.cell_ycrd) as cell_ycrd_max
FROM BDPVIEW.L1DA_TB_CEBY_CUST_MVMT_H AS TCCM
WHERE 1=1
AND TCCM.p_basis_yyyy = 2022 
AND TCCM.p_basis_mm = 1
AND (TCCM.p_basis_dd BETWEEN 10 AND 14)
AND FROM_TIMESTAMP(TCCM.mt10_unit_tm,'HH') = '13'
GROUP BY imsi_no, yyyymmdd
;

------------------------------
-- 명세정보 및 일별 재택근무 추정여부
------------------------------
TRUNCATE TABLE IF EXISTS SBXL2CK.L2CK_HB5EO_REFINED_TARGET_TEMP;
DROP TABLE IF EXISTS SBXL2CK.L2CK_HB5EO_REFINED_TARGET_TEMP;
CREATE TABLE SBXL2CK.L2CK_HB5EO_REFINED_TARGET_TEMP 
	STORED AS PARQUET
	LOCATION '/SBX/impala_table/L2/CK/L2CK_HB5EO_REFINED_TARGET_TEMP' AS 
SELECT BASE.*
, IF(AGG.COUNT_AT_HOME>0, 1, 0) AS TRUE_AT_HOME
, AGG.COUNT_AT_HOME
, AGG.COUNT_AT_HOME / 5 AS RATE_AT_HOME
FROM SBXL2CK.L2CK_HB5EO_BASE_TARGET_TEMP AS BASE 
JOIN (
	SELECT T.imsi_no, SUM(T.true_at_home) AS COUNT_AT_HOME
	FROM (
		SELECT DLY.imsi_no, TO_TIMESTAMP(DLY.yyyymmdd, 'yyyyMMdd') as yyyymmdd
		, IF(RSDC.rsdc_xcrd_addr BETWEEN DLY.cell_xcrd_min - RSDC.sstd_xcrd 
								AND DLY.cell_xcrd_max + RSDC.sstd_xcrd, 1, 0) 
		* IF(RSDC.rsdc_ycrd_addr BETWEEN DLY.cell_ycrd_min - RSDC.sstd_ycrd
								AND DLY.cell_ycrd_max + RSDC.sstd_ycrd, 1, 0)
		 AS true_at_home
		FROM SBXL2CK.L2CK_HB5EO_DLY_DAYTIME_ACTIVE_RANGE_TEMP AS DLY
		JOIN SBXL2CK.L2CK_HB5EO_BASE_RSDC_SSTD_TEMP AS RSDC ON RSDC.imsi_no = DLY.imsi_no
		WHERE 1=1
	) AS T
	WHERE 1=1
	GROUP BY 1
) AS AGG ON AGG.imsi_no = BASE.imsi_no
WHERE 1=1
;


------------------------------
-- 명세정보 및 일별 재택근무 추정여부 (성별/연령대별/가구특성/재택추정여부 1000명씩 층화추출)
------------------------------
TRUNCATE TABLE IF EXISTS SBXL2CK.L2CK_HB5EO_SAMPLED_TARGET_TEMP;
DROP TABLE IF EXISTS SBXL2CK.L2CK_HB5EO_SAMPLED_TARGET_TEMP;
CREATE TABLE SBXL2CK.L2CK_HB5EO_SAMPLED_TARGET_TEMP 
	STORED AS PARQUET
	LOCATION '/SBX/impala_table/L2/CK/L2CK_HB5EO_SAMPLED_TARGET_TEMP' AS 
SELECT P.entr_no, P.cust_no, P.imsi_no
, P.sex_dv_cd, P.cust_age_idx
, P.hse_id, P.hse_sgmt_cd, P.hse_sgmt_nm
, P.rsdc_xcrd_addr, P.rsdc_ycrd_addr
, P.true_at_home, P.count_at_home, P.rate_at_home
FROM (
	SELECT T.*
	, ROW_NUMBER() OVER(
		PARTITION BY T.sex_dv_cd, T.cust_age_idx, T.hse_sgmt_cd, T.true_at_home
		ORDER BY T.sex_dv_cd, T.cust_age_idx, T.hse_sgmt_cd, T.true_at_home, RAND(100) -- seed
	) AS row_num
	FROM SBXL2CK.L2CK_HB5EO_REFINED_TARGET_TEMP AS T
	WHERE 1=1
) AS P
WHERE 1=1
AND P.row_num BETWEEN 1 AND 1000;

