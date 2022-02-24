------------------------------------------
-- Target 2 Site CNC 데이터 생성
-- 사용자별 주간 CNC 총사용량 (Base 단계, outlier 제거 전)
-- Count: 67,195 건
------------------------------------------

--SET MEM_LIMIT= '7g';
--SET BATCH_SIZE = 10;

TRUNCATE TABLE IF EXISTS SBXL2CK.L2CK_HB5EO_TARGET2SITE_CNC_TEMP;
DROP TABLE IF EXISTS SBXL2CK.L2CK_HB5EO_TARGET2SITE_CNC_TEMP;
CREATE TABLE SBXL2CK.L2CK_HB5EO_TARGET2SITE_CNC_TEMP 
	STORED AS PARQUET
	LOCATION '/SBX/impala_table/L2/CK/L2CK_HB5EO_TARGET2SITE_CNC_TEMP' AS 
SELECT CNC.imsi_no, SITE.site_id
, SUM(CNC.asmp_vsit_tmsc * CNC.asmp_vsit_tm) AS use_amount
, COUNT(CNC.p_basis_dd) AS visit_days
FROM BDPVIEW.L1DA_MBL_CNC_INFO_CLSS_DALY_S AS CNC
JOIN SBXL2CK.L2CK_HB5EO_SAMPLED_TARGET_TEMP AS TARGET ON TARGET.imsi_no = CNC.imsi_no
JOIN SBXL2CK.L2CK_HB5EO_BASE_SITE_TEMP AS SITE ON SITE.site_nm = CNC.site_nm
AND CNC.p_basis_yyyy = 2022 
AND CNC.p_basis_mm = 1
AND (CNC.p_basis_dd BETWEEN 10 AND 14)
AND (CNC.asmp_vsit_tmsc > 4 OR CNC.asmp_vsit_tm > CNC.asmp_vsit_tmsc*0.5)
GROUP BY 1,2
HAVING use_amount >= 5 AND visit_days > 1
-- LIMIT 50
;

