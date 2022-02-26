--------------
-- 1. MY 테이블
--------------
SELECT C.*
FROM SBXL2CK.L2CK_HB5EO_MASTER_TARGET2SITE_CNC_TEMP AS C
JOIN SBXL2CK.L2CK_HB5EO_MASTER_TARGET_TEMP AS T ON T.imsi_no = C.imsi_no
WHERE 1=1
AND T.cust_age_idx = 4 AND T.hse_sgmt_cd = '20201'
AND T.sex_dv_cd = 'MALE'
AND T.true_at_home = 1
;

--------------
-- 1. MN 테이블
--------------
SELECT C.*
FROM SBXL2CK.L2CK_HB5EO_MASTER_TARGET2SITE_CNC_TEMP AS C
JOIN SBXL2CK.L2CK_HB5EO_MASTER_TARGET_TEMP AS T ON T.imsi_no = C.imsi_no
WHERE 1=1
AND T.cust_age_idx = 4 AND T.hse_sgmt_cd = '20201'
AND T.sex_dv_cd = 'MALE'
AND T.true_at_home = 0
;
--------------
-- 1. FY 테이블
--------------
SELECT C.*
FROM SBXL2CK.L2CK_HB5EO_MASTER_TARGET2SITE_CNC_TEMP AS C
JOIN SBXL2CK.L2CK_HB5EO_MASTER_TARGET_TEMP AS T ON T.imsi_no = C.imsi_no
WHERE 1=1
AND T.cust_age_idx = 4 AND T.hse_sgmt_cd = '20201'
AND T.sex_dv_cd = 'FEMALE'
AND T.true_at_home = 1
;
--------------
-- 1. FN 테이블
--------------
SELECT C.*
FROM SBXL2CK.L2CK_HB5EO_MASTER_TARGET2SITE_CNC_TEMP AS C
JOIN SBXL2CK.L2CK_HB5EO_MASTER_TARGET_TEMP AS T ON T.imsi_no = C.imsi_no
WHERE 1=1
AND T.cust_age_idx = 4 AND T.hse_sgmt_cd = '20201'
AND T.sex_dv_cd = 'FEMALE'
AND T.true_at_home = 0
;

--------------
-- 1. Site 테이블
--------------
SELECT * FROM SBXL2CK.L2CK_HB5EO_MASTER_SITE_TEMP;

