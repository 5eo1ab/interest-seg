------------------------------------------
-- Target Master 데이터 생성 
-- 테이블명: 	SBXL2CK.L2CK_HB5EO_MASTER_TARGET_TEMP 
-- 직전 테이블명:		SBXL2CK.L2CK_HB5EO_REFINED_TARGET_TEMP 
-- Count: 		3,966,698 건  ->		70,525 건
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
JOIN BDPVIEW.L2EOT_CUST_IDVL_IDEX_M AS CII ON CII.entr_no = MEMT.entr_no
WHERE 1=1
-- 기준년월: 2022년 1월
AND MEMT.base_yymm = '202201'
AND LOC.p_basis_yyyy = 2022 AND LOC.p_basis_mm = 1
AND HSE.p_basis_yyyy = 2022 AND HSE.p_basis_mm = 1
AND CII.p_basis_yyyy = 2022 AND CII.p_basis_mm = 1
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
AND CII.offc_wkr_yn = 1
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
, MAX(TCCM.cell_ycrd) as cell_ycrd_max
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
-- 명세정보 및 일별 재택근무 추정여부 (고객그룹별 1천명씩 층화추출)
------------------------------
TRUNCATE TABLE IF EXISTS SBXL2CK.L2CK_HB5EO_MASTER_TARGET_TEMP;
DROP TABLE IF EXISTS SBXL2CK.L2CK_HB5EO_MASTER_TARGET_TEMP;
CREATE TABLE SBXL2CK.L2CK_HB5EO_MASTER_TARGET_TEMP 
	STORED AS PARQUET
	LOCATION '/SBX/impala_table/L2/CK/L2CK_HB5EO_MASTER_TARGET_TEMP' AS 
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
		-- PARTITION BY T.sex_dv_cd, T.true_at_home
		-- ORDER BY T.sex_dv_cd, T.true_at_home, RAND(100) -- seed
	) AS row_num
	FROM SBXL2CK.L2CK_HB5EO_REFINED_TARGET_TEMP AS T
	WHERE 1=1
) AS P
WHERE 1=1
-- AND P.row_num BETWEEN 1 AND 1000
;


------------------------------------------
-- Site (Refine) 데이터 생성
-- 테이블명: 	SBXL2CK.L2CK_HB5EO_REFINED_TARGET_TEMP
-- Count: 8,640 건
-- base 조건:
-- - 카테고리 미분류 (2022년1월31일 기준)
-- - 필터링 대상 사이트 제외
------------------------------------------

--SET MEM_LIMIT= '7g';
--SET BATCH_SIZE = 10;

TRUNCATE TABLE IF EXISTS SBXL2CK.L2CK_HB5EO_BASE_SITE_TEMP;
DROP TABLE IF EXISTS SBXL2CK.L2CK_HB5EO_BASE_SITE_TEMP;
CREATE TABLE SBXL2CK.L2CK_HB5EO_BASE_SITE_TEMP 
	STORED AS PARQUET
	LOCATION '/SBX/impala_table/L2/CK/L2CK_HB5EO_BASE_SITE_TEMP' AS 
SELECT CONCAT('s', CAST(T.site_num AS STRING)) AS site_id
, T.site_nm
, T.catg_lv1
, T.catg_lv2
, T.catg_lv3
FROM (
	SELECT BASE.*
	, ROW_NUMBER() OVER (ORDER BY BASE.host_addr_cnt DESC, BASE.site_nm) AS site_num
	FROM (
		SELECT HOST.site_nm
		, HOST.cnc_info_catg_lvl1_nm AS catg_lv1
		, HOST.cnc_info_catg_lvl2_nm AS catg_lv2
		, HOST.cnc_info_catg_lvl3_nm AS catg_lv3
		, COUNT(HOST.host_addr) AS host_addr_cnt
		FROM BDPVIEW.L1DA_HOST_INFO_M AS HOST
		WHERE 1=1
		AND HOST.p_basis_yyyy = 2022
		AND HOST.p_basis_mm = 1
		AND HOST.p_basis_dd = 31
		AND HOST.cnc_info_catg_lvl1_nm IS NOT NULL
		AND HOST.cnc_info_catg_lvl1_nm NOT IN ('기타서비스','기업서비스')
		AND HOST.cnc_info_catg_lvl2_nm NOT IN ('날씨')
		AND HOST.cnc_info_catg_lvl3_nm NOT IN ('20-30대 여성')
		AND HOST.site_nm NOT IN (
			'LGU+ TSM',
			'AhnLab',
			'Microsoft Office',
			'Warning',
			'LGU+ 배움마당',
			'iTunes-Apple',
			'QQ',
			'NHN TOAST',
			'Facebook for Developers',
			'Outlook',
			'네이버 개발자 센터',
			'Apple',
			'iCloud',
			'Google 어카운트',
			'jsDelivr',
			'jQuery',
			'Criteo',
			'Unity',
			'RawGit',
			'Taboola',
			'퀸잇',
			'McAfee',
			'라온시큐어',
			'ONE store',
			'Google Marketing Platform',
			'애드몰',
			'Buzzvil',
			'디지털캠프',
			'채널톡',
			'Google Play',
			'ZigZag',
			'OneDrive',
			'Chrome 웹브라우저',
			'1boon',
			'COOV-질병관리청백신접종증명',
			'질병관리청',
			'질병관리본부 예방접종도우미',
			'질병관리본부', 
			'코로나19 예방접종 사전예약 시스템',
			'코로나 라이브',
			'성동구 코로나',
			'코로나굿닥',
			'코로나19 예방접종',
			'코로나19상황판',
			'청주시 코로나 상황판',
			'recovercovid',
			'SKT T전화',
			'LG UCAP Messenger',
			'시지온'
		)
		GROUP BY 1,2,3,4
	) AS BASE
	WHERE 1=1
) AS T
WHERE 1=1
;


------------------------------------------
-- Target 2 Site CNC (Base) 데이터 생성
-- 사용자별 주간 CNC 총사용량 (Outlier 제거 전)
-- Count: 67,195 건
------------------------------------------

--SET MEM_LIMIT= '7g';
--SET BATCH_SIZE = 10;

TRUNCATE TABLE IF EXISTS SBXL2CK.L2CK_HB5EO_BASE_TARGET2SITE_CNC_TEMP;
DROP TABLE IF EXISTS SBXL2CK.L2CK_HB5EO_BASE_TARGET2SITE_CNC_TEMP;
CREATE TABLE SBXL2CK.L2CK_HB5EO_BASE_TARGET2SITE_CNC_TEMP 
	STORED AS PARQUET
	LOCATION '/SBX/impala_table/L2/CK/L2CK_HB5EO_BASE_TARGET2SITE_CNC_TEMP' AS 
SELECT CNC.imsi_no, SITE.site_id
, SUM(CNC.asmp_vsit_tmsc * CNC.asmp_vsit_tm) AS use_amount
, COUNT(CNC.p_basis_dd) AS visit_days
FROM BDPVIEW.L1DA_MBL_CNC_INFO_CLSS_DALY_S AS CNC
JOIN SBXL2CK.L2CK_HB5EO_MASTER_TARGET_TEMP AS TARGET ON TARGET.imsi_no = CNC.imsi_no
JOIN SBXL2CK.L2CK_HB5EO_BASE_SITE_TEMP AS SITE ON SITE.site_nm = CNC.site_nm
AND CNC.p_basis_yyyy = 2022 
AND CNC.p_basis_mm = 1
AND (CNC.p_basis_dd BETWEEN 10 AND 14)
AND (CNC.asmp_vsit_tmsc > 4 OR CNC.asmp_vsit_tm > CNC.asmp_vsit_tmsc*0.5)

GROUP BY 1,2
HAVING use_amount >= 5 AND visit_days > 1
;


-----------------------------
-- Site Master 데이터 생성
-- Count; 1,858 개 	-> 1,846 개 (Site master)

-- Target2Site Master 데이터 생성
-- Count: 1,403,999 건
-----------------------------

TRUNCATE TABLE IF EXISTS SBXL2CK.L2CK_HB5EO_REFINED_SITE_TEMP;
DROP TABLE IF EXISTS SBXL2CK.L2CK_HB5EO_REFINED_SITE_TEMP;
CREATE TABLE SBXL2CK.L2CK_HB5EO_REFINED_SITE_TEMP 
	STORED AS PARQUET
	LOCATION '/SBX/impala_table/L2/CK/L2CK_HB5EO_REFINED_SITE_TEMP' AS 
SELECT T.site_id
, T.site_nm
, T.catg_lv1
, T.catg_lv2
, T.catg_lv3
, C.count_occur
, C.avg_use_amount
, C.std_use_amount
, C.min_use_amount
, C.max_use_amount
FROM SBXL2CK.L2CK_HB5EO_BASE_SITE_TEMP AS T
JOIN (
	SELECT C.site_id
	, COUNT(C.imsi_no) AS count_occur
	, AVG(C.use_amount) AS avg_use_amount
	, STDDEV(C.use_amount) AS std_use_amount
	, MIN(C.use_amount) AS min_use_amount
	, MAX(C.use_amount) AS max_use_amount
	FROM SBXL2CK.L2CK_HB5EO_BASE_TARGET2SITE_CNC_TEMP AS C
	WHERE 1=1
	GROUP BY 1
	HAVING count_occur > 1
) AS C ON C.site_id = T.site_id
WHERE 1=1
;


TRUNCATE TABLE IF EXISTS SBXL2CK.L2CK_HB5EO_MASTER_TARGET2SITE_CNC_TEMP;
DROP TABLE IF EXISTS SBXL2CK.L2CK_HB5EO_MASTER_TARGET2SITE_CNC_TEMP;
CREATE TABLE SBXL2CK.L2CK_HB5EO_MASTER_TARGET2SITE_CNC_TEMP 
	STORED AS PARQUET
	LOCATION '/SBX/impala_table/L2/CK/L2CK_HB5EO_MASTER_TARGET2SITE_CNC_TEMP' AS 
SELECT CNC.imsi_no, CNC.site_id, CNC.use_amount
FROM SBXL2CK.L2CK_HB5EO_BASE_TARGET2SITE_CNC_TEMP AS CNC
JOIN SBXL2CK.L2CK_HB5EO_REFINED_SITE_TEMP AS SITE ON SITE.site_id = CNC.site_id
WHERE 1=1
AND CNC.use_amount > (SITE.avg_use_amount - SITE.std_use_amount*2)
AND CNC.use_amount < (SITE.avg_use_amount + SITE.std_use_amount*2)
;


TRUNCATE TABLE IF EXISTS SBXL2CK.L2CK_HB5EO_MASTER_SITE_TEMP;
DROP TABLE IF EXISTS SBXL2CK.L2CK_HB5EO_MASTER_SITE_TEMP;
CREATE TABLE SBXL2CK.L2CK_HB5EO_MASTER_SITE_TEMP 
	STORED AS PARQUET
	LOCATION '/SBX/impala_table/L2/CK/L2CK_HB5EO_MASTER_SITE_TEMP' AS 
SELECT SITE.*
FROM SBXL2CK.L2CK_HB5EO_REFINED_SITE_TEMP AS SITE
JOIN (
SELECT DISTINCT site_id FROM SBXL2CK.L2CK_HB5EO_MASTER_TARGET2SITE_CNC_TEMP
) AS CNC ON CNC.site_id = SITE.site_id
;