CREATE OR REPLACE PROCEDURE ACCDWH_UDW.sp_accme_entry_fact
AS
   CURSOR c1
   IS
      SELECT eod_processed_date_sk AS v_time,
             EXTRACT (MONTH FROM eod_processed_date) AS v_month,
             EXTRACT (YEAR FROM eod_processed_date) AS v_year
        FROM accdwh_com.data_processed_time;

   CURSOR c2 (v_time NUMBER, v_month VARCHAR, v_year VARCHAR)
   IS
      -- QUERY PER AREA --
      SELECT tmp4.time_sk, tmp4.branch_sk, tmp4.time_upload,
             tmp4.target_area_desc, tmp4.branch_desc, tmp4.branch_code,
             tmp4.person_name, tmp4.total_entry, tmp4.total_entry_aol,
             tmp4.total_entry_accme, tmp4.percentage_aol,
             tmp4.percentage_accme, tmp4.type_dashboard
        FROM (SELECT v_time AS time_sk, '' AS branch_sk,
                       CURRENT_DATE AS time_upload, tmp3.target_area_desc,
                       '' AS branch_desc, '' AS branch_code,
                       '' AS person_name,
                       SUM (tmp3.total_entry) AS total_entry,
                       SUM (tmp3.total_entry_aol) AS total_entry_aol,
                       SUM (tmp3.total_entry_accme) AS total_entry_accme,
                         DECODE
                               (SUM (tmp3.total_entry),
                                0, 0,
                                (  SUM (tmp3.total_entry_aol)
                                 / SUM (tmp3.total_entry)
                                )
                               )
                       * 100 AS percentage_aol,
                         DECODE
                            (SUM (tmp3.total_entry),
                             0, 0,
                             (  SUM (tmp3.total_entry_accme)
                              / SUM (tmp3.total_entry)
                             )
                            )
                       * 100 AS percentage_accme,
                       'ENTRY_AREA' AS type_dashboard
                  FROM (SELECT   tmp2.target_area_desc,
                                 COUNT (tmp2.target_area_desc) AS total_entry,
                                 (CASE
                                     WHEN tmp2.type_entry = 'AOL'
                                        THEN COUNT (tmp2.target_area_desc)
                                     ELSE 0
                                  END
                                 ) AS total_entry_aol,
                                 (CASE
                                     WHEN tmp2.type_entry = 'ACCME'
                                        THEN COUNT (tmp2.target_area_desc)
                                     ELSE 0
                                  END
                                 ) AS total_entry_accme
                            FROM (SELECT DISTINCT (tmp.no_registration),
                                                  tmp.st_appl, tmp.dt_appl,
                                                  tmp.cd_sp, tmp.desc_sp,
                                                  tmp.target_area_desc,
                                                  tmp.id_user_added,
                                                  tmp.name_user,
                                                  tmp.type_entry
                                             FROM (SELECT DISTINCT (a.no_registration
                                                                   ),
                                                                   a.st_appl,
                                                                   a.dt_appl,
                                                                   a.id_user_added,
                                                                   c.name_user,
                                                                   a.cd_sp,
                                                                   d.desc_sp,
                                                                   ee.target_area_desc,
                                                                   'ACCME' AS type_entry
                                                              FROM mtr_accals.trn_appl_detail@aol_prod a 
                                                              LEFT JOIN mtr_accals.mst_user@aol_prod c ON a.id_user_added = c.id_user
                                                              LEFT JOIN mtr_accals.mst_sp@aol_prod d ON a.cd_sp = d.cd_sp
                                                              LEFT JOIN accdwh_com.branch ee ON ee.branch_code =d.cd_sp
                                                             WHERE a.id_user_added IS NOT NULL
                                                               AND TO_NUMBER (TO_CHAR(a.dt_appl,'YYYYMMDD')) 
                                                               BETWEEN  substr(v_time,1,6)||'01' AND v_time
                                                               AND ee.flag_active = 'Y'
                                                                AND d.desc_sp not like 'HRD%' 
                                                                AND d.desc_sp not like 'FLEET%'
                                                                AND d.desc_sp not like 'FIF%'
                                                                AND d.desc_sp not like '%PRODA'
                                                                AND d.desc_sp not like 'COMMERCIAL%'
                                                                AND d.desc_sp not like 'CENTRALIZED SURVEY%'
                                                                AND d.desc_sp not like 'PONTIANAK AVALIS'
                                                                AND d.desc_sp not like 'HEAD OFFICE'
                                                                AND d.desc_sp not like 'COLLECTION CENTRO%'
                                                                AND d.desc_sp IN (
                                                                    SELECT DISTINCT BRANCH_DESC
                                                                    FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT
                                                                    WHERE STATUS = 'Implement' AND TIME_SK = (SELECT MAX(X.TIME_SK) 
                                                                    FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT X)
                                                                )
                                                               AND a.no_registration IN (
                                                                      SELECT no_registration
                                                                        FROM mtr_accme.trn_activity_log@aol_prod
                                                                       WHERE no_registration IS NOT NULL)
                                                   UNION ALL
                                                   SELECT DISTINCT (a.no_registration
                                                                   ),
                                                                   a.st_appl,
                                                                   a.dt_appl,
                                                                   a.id_user_added,
                                                                   c.name_user,
                                                                   a.cd_sp,
                                                                   d.desc_sp,
                                                                   ee.target_area_desc,
                                                                   'AOL'
                                                                      AS type_entry
                                                              FROM mtr_accals.trn_appl_detail@aol_prod a 
                                                                LEFT JOIN mtr_accals.mst_user@aol_prod c ON a.id_user_added = c.id_user
                                                                LEFT JOIN mtr_accals.mst_sp@aol_prod d ON a.cd_sp = d.cd_sp
                                                                LEFT JOIN accdwh_com.branch ee ON ee.branch_code = d.cd_sp
                                                             WHERE a.id_user_added IS NOT NULL
                                                               AND TO_NUMBER (TO_CHAR(a.dt_appl,'YYYYMMDD')) 
                                                               BETWEEN  substr(v_time,1,6)||'01' AND v_time
                                                               AND ee.flag_active = 'Y'
                                                               AND ee.flag_active = 'Y'
                                                                AND d.desc_sp not like 'HRD%' 
                                                                AND d.desc_sp not like 'FLEET%'
                                                                AND d.desc_sp not like 'FIF%'
                                                                AND d.desc_sp not like '%PRODA'
                                                                AND d.desc_sp not like 'COMMERCIAL%'
                                                                AND d.desc_sp not like 'CENTRALIZED SURVEY%'
                                                                AND d.desc_sp not like 'PONTIANAK AVALIS'
                                                                AND d.desc_sp not like 'HEAD OFFICE'
                                                                AND d.desc_sp not like 'COLLECTION CENTRO%'
                                                                AND d.desc_sp IN (
                                                                    SELECT DISTINCT BRANCH_DESC
                                                                    FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT
                                                                    WHERE STATUS = 'Implement' AND TIME_SK = (SELECT MAX(X.TIME_SK) 
                                                                    FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT X)
                                                                )
                                                               AND a.no_registration NOT IN (
                                                                      SELECT no_registration
                                                                        FROM mtr_accme.trn_activity_log@aol_prod
                                                                       WHERE no_registration IS NOT NULL)) tmp
                                         ORDER BY tmp.target_area_desc,
                                                  tmp.desc_sp) tmp2
                        GROUP BY tmp2.target_area_desc, tmp2.type_entry) tmp3
              GROUP BY tmp3.target_area_desc
              ORDER BY tmp3.target_area_desc) tmp4
      UNION ALL
      -- QUERY PER BRANCH --
      SELECT tmp4.time_sk, tmp4.branch_sk, tmp4.time_upload,
             tmp4.target_area_desc, tmp4.branch_desc, tmp4.branch_code,
             tmp4.person_name, tmp4.total_entry, tmp4.total_entry_aol,
             tmp4.total_entry_accme, tmp4.percentage_aol,
             tmp4.percentage_accme, tmp4.type_dashboard
        FROM (SELECT   v_time AS time_sk, '' AS branch_sk,
                       CURRENT_DATE AS time_upload, '' AS target_area_desc,
                       tmp3.desc_sp AS branch_desc, tmp3.cd_sp AS branch_code,
                       '' AS person_name,
                       SUM (tmp3.total_entry) AS total_entry,
                       SUM (tmp3.total_entry_aol) AS total_entry_aol,
                       SUM (tmp3.total_entry_accme) AS total_entry_accme,
                         DECODE
                               (SUM (tmp3.total_entry),
                                0, 0,
                                (  SUM (tmp3.total_entry_aol)
                                 / SUM (tmp3.total_entry)
                                )
                               )
                       * 100 AS percentage_aol,
                         DECODE
                            (SUM (tmp3.total_entry),
                             0, 0,
                             (  SUM (tmp3.total_entry_accme)
                              / SUM (tmp3.total_entry)
                             )
                            )
                       * 100 AS percentage_accme,
                       'ENTRY_CABANG' AS type_dashboard
                  FROM (SELECT   tmp2.cd_sp, tmp2.desc_sp,
                                 COUNT (tmp2.desc_sp) AS total_entry,
                                 (CASE
                                     WHEN tmp2.type_entry = 'AOL'
                                        THEN COUNT (tmp2.desc_sp)
                                     ELSE 0
                                  END
                                 ) AS total_entry_aol,
                                 (CASE
                                     WHEN tmp2.type_entry = 'ACCME'
                                        THEN COUNT (tmp2.desc_sp)
                                     ELSE 0
                                  END
                                 ) AS total_entry_accme
                            FROM (SELECT DISTINCT (tmp.no_registration),
                                                  tmp.st_appl, tmp.dt_appl,
                                                  tmp.cd_sp, tmp.desc_sp,
                                                  tmp.target_area_desc,
                                                  tmp.id_user_added,
                                                  tmp.name_user,
                                                  tmp.type_entry
                                             FROM (SELECT DISTINCT (a.no_registration
                                                                   ),
                                                                   a.st_appl,
                                                                   a.dt_appl,
                                                                   a.id_user_added,
                                                                   c.name_user,
                                                                   a.cd_sp,
                                                                   d.desc_sp,
                                                                   ee.target_area_desc,
                                                                   'ACCME'
                                                                      AS type_entry
                                                              FROM mtr_accals.trn_appl_detail@aol_prod a
                                                               LEFT JOIN mtr_accals.mst_user@aol_prod c ON a.id_user_added = c.id_user
                                                               LEFT JOIN mtr_accals.mst_sp@aol_prod d ON a.cd_sp = d.cd_sp
                                                               LEFT JOIN accdwh_com.branch ee ON ee.branch_code = d.cd_sp
                                                             WHERE a.id_user_added IS NOT NULL
                                                               AND TO_NUMBER (TO_CHAR(a.dt_appl,'YYYYMMDD')) 
                                                               BETWEEN  substr(v_time,1,6)||'01' AND v_time
                                                               AND ee.flag_active = 'Y'
                                                                AND d.desc_sp not like 'HRD%' 
                                                                AND d.desc_sp not like 'FLEET%'
                                                                AND d.desc_sp not like 'FIF%'
                                                                AND d.desc_sp not like '%PRODA'
                                                                AND d.desc_sp not like 'COMMERCIAL%'
                                                                AND d.desc_sp not like 'CENTRALIZED SURVEY%'
                                                                AND d.desc_sp not like 'PONTIANAK AVALIS'
                                                                AND d.desc_sp not like 'HEAD OFFICE'
                                                                AND d.desc_sp not like 'COLLECTION CENTRO%'
                                                                AND d.desc_sp IN (
                                                                    SELECT DISTINCT BRANCH_DESC
                                                                    FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT
                                                                    WHERE STATUS = 'Implement' AND TIME_SK = (SELECT MAX(X.TIME_SK) 
                                                                    FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT X)
                                                                )
                                                               AND a.no_registration IN (
                                                                      SELECT no_registration
                                                                        FROM mtr_accme.trn_activity_log@aol_prod
                                                                       WHERE no_registration IS NOT NULL)
                                                   UNION ALL
                                                   SELECT DISTINCT (a.no_registration
                                                                   ),
                                                                   a.st_appl,
                                                                   a.dt_appl,
                                                                   a.id_user_added,
                                                                   c.name_user,
                                                                   a.cd_sp,
                                                                   d.desc_sp,
                                                                   ee.target_area_desc,
                                                                   'AOL'
                                                                      AS type_entry
                                                              FROM mtr_accals.trn_appl_detail@aol_prod a 
                                                                LEFT JOIN mtr_accals.mst_user@aol_prod c ON a.id_user_added = c.id_user
                                                                LEFT JOIN mtr_accals.mst_sp@aol_prod d ON a.cd_sp = d.cd_sp
                                                                LEFT JOIN accdwh_com.branch ee ON ee.branch_code = d.cd_sp
                                                             WHERE a.id_user_added IS NOT NULL
                                                               AND TO_NUMBER (TO_CHAR(a.dt_appl,'YYYYMMDD')) 
                                                               BETWEEN  substr(v_time,1,6)||'01' AND v_time
                                                               AND ee.flag_active = 'Y'
                                                                AND d.desc_sp not like 'HRD%' 
                                                                AND d.desc_sp not like 'FLEET%'
                                                                AND d.desc_sp not like 'FIF%'
                                                                AND d.desc_sp not like '%PRODA'
                                                                AND d.desc_sp not like 'COMMERCIAL%'
                                                                AND d.desc_sp not like 'CENTRALIZED SURVEY%'
                                                                AND d.desc_sp not like 'PONTIANAK AVALIS'
                                                                AND d.desc_sp not like 'HEAD OFFICE'
                                                                AND d.desc_sp not like 'COLLECTION CENTRO%'
                                                                AND d.desc_sp IN (
                                                                    SELECT DISTINCT BRANCH_DESC
                                                                    FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT
                                                                    WHERE STATUS = 'Implement' AND TIME_SK = (SELECT MAX(X.TIME_SK) 
                                                                    FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT X)
                                                                )
                                                               AND a.no_registration NOT IN (
                                                                      SELECT no_registration
                                                                        FROM mtr_accme.trn_activity_log@aol_prod
                                                                       WHERE no_registration IS NOT NULL)) tmp
                                         ORDER BY tmp.cd_sp, tmp.desc_sp) tmp2
                        GROUP BY tmp2.cd_sp, tmp2.desc_sp, tmp2.type_entry) tmp3
              GROUP BY tmp3.cd_sp, tmp3.desc_sp
              ORDER BY tmp3.cd_sp, tmp3.desc_sp) tmp4
      UNION ALL
      -- QUERY PER SALES --
      SELECT tmp4.time_sk, tmp4.branch_sk, tmp4.time_upload,
             tmp4.target_area_desc, tmp4.branch_desc, tmp4.branch_code,
             tmp4.person_name, tmp4.total_entry, tmp4.total_entry_aol,
             tmp4.total_entry_accme, tmp4.percentage_aol,
             tmp4.percentage_accme, tmp4.type_dashboard
        FROM (SELECT   v_time AS time_sk, '' AS branch_sk,
                       CURRENT_DATE AS time_upload, '' AS target_area_desc,
                       tmp3.desc_sp AS branch_desc, tmp3.cd_sp AS branch_code,
                       tmp3.name_user AS person_name,
                       SUM (tmp3.total_entry) AS total_entry,
                       SUM (tmp3.total_entry_aol) AS total_entry_aol,
                       SUM (tmp3.total_entry_accme) AS total_entry_accme,
                         DECODE
                               (SUM (tmp3.total_entry),
                                0, 0,
                                (  SUM (tmp3.total_entry_aol)
                                 / SUM (tmp3.total_entry)
                                )
                               )
                       * 100 AS percentage_aol,
                         DECODE
                            (SUM (tmp3.total_entry),
                             0, 0,
                             (  SUM (tmp3.total_entry_accme)
                              / SUM (tmp3.total_entry)
                             )
                            )
                       * 100 AS percentage_accme,
                       'ENTRY_CABANG_SALES' AS type_dashboard
                  FROM (SELECT   tmp2.desc_sp, tmp2.cd_sp, tmp2.name_user,
                                 COUNT (tmp2.desc_sp) AS total_entry,
                                 (CASE
                                     WHEN tmp2.type_entry = 'AOL'
                                        THEN COUNT (tmp2.desc_sp)
                                     ELSE 0
                                  END
                                 ) AS total_entry_aol,
                                 (CASE
                                     WHEN tmp2.type_entry = 'ACCME'
                                        THEN COUNT (tmp2.desc_sp)
                                     ELSE 0
                                  END
                                 ) AS total_entry_accme
                            FROM (SELECT DISTINCT (tmp.no_registration),
                                                  tmp.st_appl, tmp.dt_appl,
                                                  tmp.cd_sp, tmp.desc_sp,
                                                  tmp.target_area_desc,
                                                  tmp.id_user_added,
                                                  tmp.name_user,
                                                  tmp.type_entry
                                             FROM (SELECT DISTINCT (a.no_registration
                                                                   ),
                                                                   a.st_appl,
                                                                   a.dt_appl,
                                                                   a.id_user_added,
                                                                   c.name_user,
                                                                   a.cd_sp,
                                                                   d.desc_sp,
                                                                   ee.target_area_desc,
                                                                   'ACCME'
                                                                      AS type_entry
                                                              FROM mtr_accals.trn_appl_detail@aol_prod a 
                                                                LEFT JOIN mtr_accals.mst_user@aol_prod c ON a.id_user_added = c.id_user
                                                                LEFT JOIN mtr_accals.mst_sp@aol_prod d ON a.cd_sp = d.cd_sp
                                                                LEFT JOIN accdwh_com.branch ee ON ee.branch_code = d.cd_sp
                                                             WHERE a.id_user_added IS NOT NULL
                                                               AND TO_NUMBER (TO_CHAR(a.dt_appl,'YYYYMMDD')) 
                                                               BETWEEN  substr(v_time,1,6)||'01' AND v_time
                                                                AND ee.flag_active = 'Y'
                                                                AND d.desc_sp not like 'HRD%' 
                                                                AND d.desc_sp not like 'FLEET%'
                                                                AND d.desc_sp not like 'FIF%'
                                                                AND d.desc_sp not like '%PRODA'
                                                                AND d.desc_sp not like 'COMMERCIAL%'
                                                                AND d.desc_sp not like 'CENTRALIZED SURVEY%'
                                                                AND d.desc_sp not like 'PONTIANAK AVALIS'
                                                                AND d.desc_sp not like 'HEAD OFFICE'
                                                                AND d.desc_sp not like 'COLLECTION CENTRO%'
                                                                AND d.desc_sp IN (
                                                                    SELECT DISTINCT BRANCH_DESC
                                                                    FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT
                                                                    WHERE STATUS = 'Implement' AND TIME_SK = (SELECT MAX(X.TIME_SK) 
                                                                    FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT X)
                                                                )
                                                               AND a.no_registration IN (
                                                                      SELECT no_registration
                                                                        FROM mtr_accme.trn_activity_log@aol_prod
                                                                       WHERE no_registration IS NOT NULL)
                                                   UNION ALL
                                                   SELECT DISTINCT (a.no_registration
                                                                   ),
                                                                   a.st_appl,
                                                                   a.dt_appl,
                                                                   a.id_user_added,
                                                                   c.name_user,
                                                                   a.cd_sp,
                                                                   d.desc_sp,
                                                                   ee.target_area_desc,
                                                                   'AOL'
                                                                      AS type_entry
                                                              FROM mtr_accals.trn_appl_detail@aol_prod a 
                                                                LEFT JOIN mtr_accals.mst_user@aol_prod c ON a.id_user_added = c.id_user
                                                                LEFT JOIN mtr_accals.mst_sp@aol_prod d ON a.cd_sp = d.cd_sp
                                                                LEFT JOIN accdwh_com.branch ee ON ee.branch_code = d.cd_sp
                                                             WHERE a.id_user_added IS NOT NULL
                                                               AND TO_NUMBER (TO_CHAR(a.dt_appl,'YYYYMMDD')) 
                                                               BETWEEN  substr(v_time,1,6)||'01' AND v_time
                                                               AND ee.flag_active = 'Y'
                                                               AND d.desc_sp not like 'HRD%' 
                                                               AND d.desc_sp not like 'FLEET%'
                                                               AND d.desc_sp not like 'FIF%'
                                                               AND d.desc_sp not like '%PRODA'
                                                               AND d.desc_sp not like 'COMMERCIAL%'
                                                               AND d.desc_sp not like 'CENTRALIZED SURVEY%'
                                                               AND d.desc_sp not like 'PONTIANAK AVALIS'
                                                               AND d.desc_sp not like 'HEAD OFFICE'
                                                               AND d.desc_sp not like 'COLLECTION CENTRO%'
                                                               AND d.desc_sp IN (
                                                                    SELECT DISTINCT BRANCH_DESC
                                                                    FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT
                                                                    WHERE STATUS = 'Implement' AND TIME_SK = (SELECT MAX(X.TIME_SK) 
                                                                    FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT X)
                                                               )
                                                               AND a.no_registration NOT IN (
                                                                      SELECT no_registration
                                                                        FROM mtr_accme.trn_activity_log@aol_prod
                                                                       WHERE no_registration IS NOT NULL)) tmp
                                         ORDER BY tmp.cd_sp, tmp.desc_sp) tmp2
                        GROUP BY tmp2.cd_sp,
                                 tmp2.desc_sp,
                                 tmp2.name_user,
                                 tmp2.type_entry) tmp3
              GROUP BY tmp3.cd_sp, tmp3.desc_sp, tmp3.name_user
              ORDER BY tmp3.cd_sp, tmp3.desc_sp) tmp4;
BEGIN
   COMMIT;

   FOR c1rec IN c1
   LOOP
      FOR c2rec IN c2 (c1rec.v_time, c1rec.v_month, c1rec.v_year)
      LOOP
         INSERT INTO accdwh_udw.accme_entry_fact
                     (time_sk, branch_sk, time_upload,
                      target_area_desc, branch_desc,
                      branch_code, person_name,
                      total_entry, entry_aol,
                      entry_accme, percentage_aol,
                      percentage_accme, type_dashboard, status
                     )
              VALUES (c2rec.time_sk, c2rec.branch_sk, c2rec.time_upload,
                      c2rec.target_area_desc, c2rec.branch_desc,
                      c2rec.branch_code, c2rec.person_name,
                      c2rec.total_entry, c2rec.total_entry_aol,
                      c2rec.total_entry_accme, c2rec.percentage_aol,
                      c2rec.percentage_accme, c2rec.type_dashboard, null
                     );

         BEGIN
            UPDATE accdwh_udw.accme_entry_fact a
               SET a.branch_sk =
                      (SELECT b.branch_sk
                         FROM accdwh_com.branch b
                        WHERE b.flag_active = 'Y'
                          AND a.branch_code = b.branch_code),
                   a.target_area_desc =
                      (SELECT b.target_area_desc
                         FROM accdwh_com.branch b
                        WHERE b.flag_active = 'Y'
                          AND a.branch_code = b.branch_code)
             WHERE type_dashboard <> 'ENTRY_AREA';
         END;

         COMMIT;
      END LOOP;
   END LOOP;
   
    UPDATE ACCDWH_UDW.ACCME_ENTRY_FACT Y
      SET Y.STATUS = 'Implement'
    WHERE Y.BRANCH_CODE IN (SELECT DISTINCT CD_SP 
                             FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT
                            WHERE STATUS = 'Implement'
                              AND TIME_SK = (SELECT MAX(X.TIME_SK) 
                                               FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT X));

    UPDATE ACCDWH_UDW.ACCME_ENTRY_FACT Y
      SET Y.STATUS = 'Implement'
    WHERE Y.TYPE_DASHBOARD like 'ENTRY_AREA' 
      AND Y.TARGET_AREA_DESC IN ( SELECT AREA 
                            FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT
                            WHERE STATUS = 'Implement'
                              AND TIME_SK = (SELECT MAX(X.TIME_SK) 
                                               FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT X) group by AREA);

   COMMIT;
END;
/
