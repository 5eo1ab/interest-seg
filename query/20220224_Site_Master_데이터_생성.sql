------------------------------------------
-- Site Master 데이터 생성
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
--		, GROUP_CONCAT(HOST.host_addr, '; ') AS host_addr_ex
		FROM BDPVIEW.L1DA_HOST_INFO_M AS HOST
		WHERE 1=1
		AND HOST.p_basis_yyyy = 2022
		AND HOST.p_basis_mm = 1
		AND HOST.p_basis_dd = 31
		AND HOST.cnc_info_catg_lvl1_nm IS NOT NULL
		AND HOST.cnc_info_catg_lvl1_nm NOT IN ('기타서비스','기업서비스')
		AND HOST.cnc_info_catg_lvl2_nm NOT IN ('날씨')
		AND HOST.cnc_info_catg_lvl3_nm NOT IN ('20-30대 여성')
		AND HOST.site_nm NOT IN ('LGU+ TSM', 'AhnLab', 'Microsoft Office', 'Warning', 'LGU+ 배움마당',
			'COOV-질병관리청백신접종증명','질병관리청', '질병관리본부 예방접종도우미', '질병관리본부', 
			'코로나19 예방접종 사전예약 시스템', '코로나 라이브', '성동구 코로나', '코로나굿닥',
			'코로나19 예방접종', '코로나19상황판', '청주시 코로나 상황판', 'recovercovid',
			'SKT T전화', 'LG UCAP Messenger', 'SnapWidget'
		)
		GROUP BY 1,2,3,4
	) AS BASE
	WHERE 1=1
) AS T
WHERE 1=1
-- LIMIT 50
;