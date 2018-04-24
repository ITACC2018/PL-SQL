CREATE OR REPLACE PROCEDURE ACCDWH_UDW.sp_accme_branch_implement_fact
AS
   CURSOR c1
   IS
      SELECT eod_processed_date_sk AS v_time
        FROM accdwh_com.data_processed_time;

   CURSOR c2 (v_time NUMBER)
   IS
      -- QUERY BRANCH IMPLEMENT ACCME --
      SELECT  v_time AS TIME_SK, d.BRANCH_SK, d.BRANCH_CODE, d.BRANCH_DESC, d.TARGET_AREA_DESC,
            (CASE
                WHEN d.BRANCH_DESC = c.DESC_SP
                    THEN 'Implement'
                ELSE ''
                END
            ) AS STATUS
      FROM MTR_ACCME.MST_USER_ACCME@aol_prod a
        LEFT JOIN mtr_accals.mst_user@aol_prod b ON a.account_id = b.id_user
        LEFT JOIN mtr_accals.mst_sp@aol_prod c ON b.cd_sp = c.cd_sp
        RIGHT JOIN accdwh_com.branch d ON c.cd_sp = d.branch_code
        WHERE d.flag_active = 'Y'
        AND d.BRANCH_CODE <> 100161 
        AND d.BRANCH_DESC NOT LIKE 'HRD%' 
        AND d.BRANCH_DESC NOT LIKE 'FLEET%'
        AND d.BRANCH_DESC NOT LIKE 'FIF%'
        AND d.BRANCH_DESC NOT LIKE '%PRODA'
        AND d.BRANCH_DESC NOT LIKE 'COMMERCIAL%'
        AND d.BRANCH_DESC NOT LIKE 'CENTRALIZED SURVEY%'
        AND d.BRANCH_DESC NOT LIKE 'PONTIANAK AVALIS'
        AND d.BRANCH_DESC NOT LIKE 'HEAD OFFICE'
        AND d.BRANCH_DESC NOT LIKE 'HRD%' 
        AND d.BRANCH_DESC NOT LIKE 'FLEET%'
        AND d.BRANCH_DESC NOT LIKE 'FIF%' 
        AND d.BRANCH_DESC NOT LIKE '%PRODA' 
        AND d.BRANCH_DESC NOT LIKE 'COMMERCIAL%' 
        AND d.BRANCH_DESC NOT LIKE 'CENTRALIZED SURVEY%'
        AND d.BRANCH_DESC <> 'PONTIANAK AVALIS' 
        AND d.BRANCH_DESC <> 'HEAD OFFICE' 
        AND d.BRANCH_DESC NOT LIKE 'COLLECTION CENTRO%' 
        GROUP BY d.TARGET_AREA_DESC, d.BRANCH_DESC, d.BRANCH_CODE, d.BRANCH_SK, c.DESC_SP;
      
BEGIN

   
   FOR c1rec IN c1
   LOOP
      DELETE ACCME_BRANCH_DET_FACT where TIME_SK = c1rec.v_time;
      FOR c2rec IN c2 (c1rec.v_time)
      LOOP
         INSERT INTO ACCDWH_UDW.ACCME_BRANCH_DET_FACT
                     (TIME_SK, BRANCH_SK, CD_SP,
                      BRANCH_DESC, STATUS, AREA
                     )
              VALUES (c2rec.TIME_SK, c2rec.BRANCH_SK, c2rec.BRANCH_CODE,
                      c2rec.BRANCH_DESC, c2rec.STATUS, c2rec.TARGET_AREA_DESC
                     );
         COMMIT;
      END LOOP;
   END LOOP;
END;
/
