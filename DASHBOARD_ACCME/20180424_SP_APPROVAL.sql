CREATE OR REPLACE PROCEDURE ACCDWH_UDW.sp_accme_approval_fact
AS
   CURSOR c1
   IS
      SELECT eod_processed_date_sk AS v_time,
             EXTRACT (MONTH FROM eod_processed_date) AS v_month,
             EXTRACT (YEAR FROM eod_processed_date) AS v_year
        FROM accdwh_com.data_processed_time;

   CURSOR c2 (v_time NUMBER, v_month VARCHAR, v_year VARCHAR)
   IS
      -- Approval Capacity Per Area --
      SELECT xx.time_sk, xx.branch_sk, xx.time_upload, xx.target_area_desc,
             xx.branch_desc, xx.branch_code, xx.group_name, xx.person_name,
             xx.total_approval, xx.approval_aol, xx.approval_accme,
             xx.percentage_aol, xx.percentage_accme, xx.type_dashboard, xx.ST_APPL_OP, xx.ST_APPL_RE, xx.TOTAL_AGGR
        FROM (SELECT DISTINCT v_time AS time_sk, NULL AS branch_sk,
                              CURRENT_DATE AS time_upload,
                              bx.target_area_desc AS target_area_desc,
                              NULL AS branch_desc, NULL AS branch_code,
                              NULL AS group_name, NULL AS person_name,
                              SUM (bx.total_approval) AS total_approval,
                              SUM (bx.aol) AS approval_aol,
                              SUM (bx.accme) AS approval_accme,
                              SUM (bx.ST_APPL_OP) AS ST_APPL_OP,
                              SUM (bx.ST_APPL_RE) AS ST_APPL_RE,
                              SUM (bx.TOTAL_AGGR) AS TOTAL_AGGR,
                              DECODE
                                  (SUM (bx.total_approval),
                                   0, 0,
                                     (SUM (bx.aol) / SUM (bx.total_approval)
                                     )
                                   * 100
                                  ) AS percentage_aol,
                              DECODE
                                 (SUM (bx.total_approval),
                                  0, 0,
                                    (SUM (bx.accme) / SUM (bx.total_approval)
                                    )
                                  * 100
                                 ) AS percentage_accme,
                              'APP_CAP_AREA' AS type_dashboard
                         FROM (SELECT   target_area_desc, st_appl, no_aggr,
                                         (CASE
                                            WHEN st_appl = 'OP' AND app_via = 'ACCMe' AND app_type = 'C'
                                               THEN COUNT (target_area_desc)
                                            ELSE 0
                                         END
                                        ) AS ST_APPL_OP,
                                           (CASE
                                            WHEN st_appl = 'RE' AND app_via = 'ACCMe' AND app_type = 'C'
                                               THEN COUNT (target_area_desc)
                                            ELSE 0
                                         END
                                        ) AS ST_APPL_RE,
                                        (CASE
                                            WHEN app_via = 'ACCMe' AND app_type = 'C'
                                               THEN COUNT (no_aggr)
                                            ELSE 0
                                         END
                                        ) AS TOTAL_AGGR,
                                        (CASE
                                            WHEN app_via = 'ACCMe' AND app_type = 'C'
                                               THEN COUNT (target_area_desc)
                                            ELSE 0
                                         END
                                        ) AS st_appl,
                                        (CASE
                                            WHEN app_type = 'C'
                                               THEN COUNT (target_area_desc)
                                            ELSE 0
                                         END
                                        ) AS total_approval,
                                        (CASE
                                            WHEN app_via = 'ACCMe'
                                            AND app_type = 'C'
                                               THEN COUNT (target_area_desc)
                                            ELSE 0
                                         END
                                        ) AS accme,
                                        (CASE
                                            WHEN app_via = 'AOL'
                                            AND app_type = 'C'
                                               THEN COUNT (target_area_desc)
                                            ELSE 0
                                         END
                                        ) AS aol
                                   FROM (SELECT  DISTINCT * /*target_area_desc, app_via,
                                                  dt_appl, app_type, st_appl, no_aggr*/
                                             FROM accdwh_udw.accme_app_det_fact
                                             WHERE (GROUP_USER IN ('SUW', 'RROH') OR ID_USER LIKE '%.H%') --SUW, RROH DAN OH--
                                             AND TO_NUMBER (TO_CHAR(dt_appl,'YYYYMMDD')) BETWEEN 
                                             substr(v_time,1,6)||'01' AND v_time
                                             AND branch_desc not like 'HRD%' 
                                             AND branch_desc not like 'FLEET%'
                                             AND branch_desc not like 'FIF%'
                                             AND branch_desc not like '%PRODA'
                                             AND branch_desc not like 'COMMERCIAL%'
                                             AND branch_desc not like 'CENTRALIZED SURVEY%'
                                             AND branch_desc not like 'PONTIANAK AVALIS'
                                             AND branch_desc not like 'HEAD OFFICE'
                                             AND branch_desc not like 'COLLECTION CENTRO%'
                                             AND branch_desc IN (SELECT DISTINCT BRANCH_DESC
                                                FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT
                                                WHERE STATUS = 'Implement'
                                                AND TIME_SK = (SELECT MAX(X.TIME_SK) 
                                             FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT X))
                                         ORDER BY name_user)
                               GROUP BY target_area_desc, app_via, app_type, st_appl, no_aggr) bx
                     GROUP BY bx.target_area_desc
                     ORDER BY bx.target_area_desc) xx
      UNION ALL
      -- Approval MAREX per Area --
      SELECT xx.time_sk, xx.branch_sk, xx.time_upload, xx.target_area_desc,
             xx.branch_desc, xx.branch_code, xx.group_name, xx.person_name,
             xx.total_approval, xx.approval_aol, xx.approval_accme,
             xx.percentage_aol, xx.percentage_accme, xx.type_dashboard, xx.ST_APPL_OP, xx.ST_APPL_RE, xx.TOTAL_AGGR
        FROM (SELECT DISTINCT v_time AS time_sk, NULL AS branch_sk,
                              CURRENT_DATE AS time_upload,
                              bx.target_area_desc AS target_area_desc,
                              NULL AS branch_desc, NULL AS branch_code,
                              NULL AS group_name, NULL AS person_name,
                              SUM (bx.total_approval) AS total_approval,
                              SUM (bx.aol) AS approval_aol,
                              SUM (bx.accme) AS approval_accme,
                              SUM (bx.ST_APPL_OP) AS ST_APPL_OP,
                              SUM (bx.ST_APPL_RE) AS ST_APPL_RE,
                              SUM (bx.TOTAL_AGGR) AS TOTAL_AGGR,
                              DECODE
                                  (SUM (bx.total_approval),
                                   0, 0,
                                     (SUM (bx.aol) / SUM (bx.total_approval)
                                     )
                                   * 100
                                  ) AS percentage_aol,
                              DECODE
                                 (SUM (bx.total_approval),
                                  0, 0,
                                    (SUM (bx.accme) / SUM (bx.total_approval)
                                    )
                                  * 100
                                 ) AS percentage_accme,
                              'APP_MAR_AREA' AS type_dashboard
                         FROM (SELECT   target_area_desc, st_appl, no_aggr,
                                                (CASE
                                            WHEN st_appl = 'OP' AND app_via = 'ACCMe' AND app_type = 'M'
                                               THEN COUNT (target_area_desc)
                                            ELSE 0
                                         END
                                        ) AS ST_APPL_OP,
                                           (CASE
                                            WHEN st_appl = 'RE' AND app_via = 'ACCMe' AND app_type = 'M'
                                               THEN COUNT (target_area_desc)
                                            ELSE 0
                                         END
                                        ) AS ST_APPL_RE,
                                        (CASE
                                            WHEN app_via = 'ACCMe' AND app_type = 'M'
                                               THEN COUNT (no_aggr)
                                            ELSE 0
                                         END
                                        ) AS TOTAL_AGGR,
                                        (CASE
                                            WHEN app_via = 'ACCMe' AND app_type = 'M'
                                               THEN COUNT (target_area_desc)
                                            ELSE 0
                                         END
                                        ) AS st_appl,
                                        (CASE
                                            WHEN app_type = 'M'
                                               THEN COUNT (target_area_desc)
                                            ELSE 0
                                         END
                                        ) AS total_approval,
                                        (CASE
                                            WHEN app_via = 'ACCMe'
                                            AND app_type = 'M'
                                               THEN COUNT (target_area_desc)
                                            ELSE 0
                                         END
                                        ) AS accme,
                                        (CASE
                                            WHEN app_via = 'AOL'
                                            AND app_type = 'M'
                                               THEN COUNT (target_area_desc)
                                            ELSE 0
                                         END
                                        ) AS aol
                                   FROM (SELECT DISTINCT * /*target_area_desc, app_via,
                                                  dt_appl, app_type, st_appl, no_aggr*/
                                             FROM accdwh_udw.accme_app_det_fact
                                             WHERE (GROUP_USER = 'RRSH' OR ID_USER LIKE '%.B%') --SUW, RROH DAN OH--
                                             AND TO_NUMBER (TO_CHAR(dt_appl,'YYYYMMDD')) BETWEEN 
                                             substr(v_time,1,6)||'01' AND v_time
                                             AND branch_desc not like 'HRD%' 
                                             AND branch_desc not like 'FLEET%'
                                             AND branch_desc not like 'FIF%'
                                             AND branch_desc not like '%PRODA'
                                             AND branch_desc not like 'COMMERCIAL%'
                                             AND branch_desc not like 'CENTRALIZED SURVEY%'
                                             AND branch_desc not like 'PONTIANAK AVALIS'
                                             AND branch_desc not like 'HEAD OFFICE'
                                             AND branch_desc not like 'COLLECTION CENTRO%'
                                             AND branch_desc IN (SELECT DISTINCT BRANCH_DESC
                                             FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT
                                             WHERE STATUS = 'Implement'
                                             AND TIME_SK = (SELECT MAX(X.TIME_SK) 
                                             FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT X))
                                         ORDER BY name_user)
                               GROUP BY target_area_desc, app_via, app_type, st_appl, no_aggr) bx
                     GROUP BY bx.target_area_desc
                     ORDER BY bx.target_area_desc) xx
      UNION ALL
      -- Approval Capacity Per Branch --
      SELECT xx.time_sk, xx.branch_sk, xx.time_upload, xx.target_area_desc,
             xx.branch_desc, xx.branch_code, xx.group_name, xx.person_name,
             xx.total_approval, xx.approval_aol, xx.approval_accme,
             xx.percentage_aol, xx.percentage_accme, xx.type_dashboard, xx.ST_APPL_OP, xx.ST_APPL_RE, xx.TOTAL_AGGR
        FROM (SELECT DISTINCT v_time AS time_sk, bx.branch_sk AS branch_sk,
                              CURRENT_DATE AS time_upload,
                              bx.target_area_desc AS target_area_desc,
                              bx.branch_desc AS branch_desc,
                              bx.cd_sp AS branch_code, NULL AS group_name,
                              NULL AS person_name,
                              SUM (bx.total_approval) AS total_approval,
                              SUM (bx.aol) AS approval_aol,
                              SUM (bx.accme) AS approval_accme,
                              SUM (bx.ST_APPL_OP) AS ST_APPL_OP,
                              SUM (bx.ST_APPL_RE) AS ST_APPL_RE,
                              SUM (bx.TOTAL_AGGR) AS TOTAL_AGGR,
                              DECODE
                                  (SUM (bx.total_approval),
                                   0, 0,
                                     (SUM (bx.aol) / SUM (bx.total_approval)
                                     )
                                   * 100
                                  ) AS percentage_aol,
                              DECODE
                                 (SUM (bx.total_approval),
                                  0, 0,
                                    (SUM (bx.accme) / SUM (bx.total_approval)
                                    )
                                  * 100
                                 ) AS percentage_accme,
                              'APP_CAP_BRANCH' AS type_dashboard
                         FROM (SELECT   target_area_desc, branch_sk,
                                        branch_desc, cd_sp, st_appl, no_aggr,
                                                (CASE
                                            WHEN st_appl = 'OP' AND app_via = 'ACCMe' AND app_type = 'C'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS ST_APPL_OP,
                                           (CASE
                                            WHEN st_appl = 'RE' AND app_via = 'ACCMe' AND app_type = 'C'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS ST_APPL_RE,
                                        (CASE
                                            WHEN app_via = 'ACCMe' AND app_type = 'C'
                                               THEN COUNT (no_aggr)
                                            ELSE 0
                                         END
                                        ) AS TOTAL_AGGR,
                                        (CASE
                                            WHEN app_via = 'ACCMe' AND app_type = 'C'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS st_appl,
                                        (CASE
                                            WHEN app_type = 'C'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS total_approval,
                                        (CASE
                                            WHEN app_via = 'ACCMe'
                                            AND app_type = 'C'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS accme,
                                        (CASE
                                            WHEN app_via = 'AOL'
                                            AND app_type = 'C'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS aol
                                   FROM (SELECT DISTINCT * /*branch_sk, branch_desc,
                                                  cd_sp, target_area_desc,
                                                  app_via, dt_appl, app_type, st_appl, no_aggr*/
                                             FROM accdwh_udw.accme_app_det_fact
                                             WHERE (GROUP_USER IN ('SUW','RROH') OR ID_USER LIKE '%.H%') --SUW, RROH DAN OH--
                                             AND TO_NUMBER (TO_CHAR(dt_appl,'YYYYMMDD')) BETWEEN substr(v_time,1,6)||'01' AND v_time
                                             AND branch_desc not like 'HRD%' 
                                             AND branch_desc not like 'FLEET%'
                                             AND branch_desc not like 'FIF%'
                                             AND branch_desc not like '%PRODA'
                                             AND branch_desc not like 'COMMERCIAL%'
                                             AND branch_desc not like 'CENTRALIZED SURVEY%'
                                             AND branch_desc not like 'PONTIANAK AVALIS'
                                             AND branch_desc not like 'HEAD OFFICE'
                                             AND branch_desc not like 'COLLECTION CENTRO%'
                                             AND branch_desc IN (SELECT DISTINCT BRANCH_DESC
                                                        FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT
                                                        WHERE STATUS = 'Implement'
                                                        AND TIME_SK = (SELECT MAX(X.TIME_SK) 
                                                     FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT X))
                                         ORDER BY name_user)
                               GROUP BY target_area_desc,
                                        branch_sk,
                                        branch_desc,
                                        cd_sp,
                                        app_via,
                                        app_type,
                                        st_appl,
                              no_aggr) bx
                     GROUP BY bx.target_area_desc,
                              bx.branch_sk,
                              bx.branch_desc,
                              bx.cd_sp
                     ORDER BY bx.target_area_desc,
                              bx.branch_sk,
                              bx.branch_desc,
                              bx.cd_sp) xx
      UNION ALL
      -- Approval Marex Per Branch --
      SELECT xx.time_sk, xx.branch_sk, xx.time_upload, xx.target_area_desc,
             xx.branch_desc, xx.branch_code, xx.group_name, xx.person_name,
             xx.total_approval, xx.approval_aol, xx.approval_accme,
             xx.percentage_aol, xx.percentage_accme, xx.type_dashboard, xx.ST_APPL_OP, xx.ST_APPL_RE, xx.TOTAL_AGGR
        FROM (SELECT DISTINCT v_time AS time_sk, bx.branch_sk AS branch_sk,
                              CURRENT_DATE AS time_upload,
                              bx.target_area_desc AS target_area_desc,
                              bx.branch_desc AS branch_desc,
                              bx.cd_sp AS branch_code, NULL AS group_name,
                              NULL AS person_name,
                              SUM (bx.total_approval) AS total_approval,
                              SUM (bx.aol) AS approval_aol,
                              SUM (bx.accme) AS approval_accme,
                              SUM (bx.ST_APPL_OP) AS ST_APPL_OP,
                              SUM (bx.ST_APPL_RE) AS ST_APPL_RE,
                              SUM (bx.TOTAL_AGGR) AS TOTAL_AGGR,
                              DECODE
                                  (SUM (bx.total_approval),
                                   0, 0,
                                     (SUM (bx.aol) / SUM (bx.total_approval)
                                     )
                                   * 100
                                  ) AS percentage_aol,
                              DECODE
                                 (SUM (bx.total_approval),
                                  0, 0,
                                    (SUM (bx.accme) / SUM (bx.total_approval)
                                    )
                                  * 100
                                 ) AS percentage_accme,
                              'APP_MAR_BRANCH' AS type_dashboard
                         FROM (SELECT   target_area_desc, branch_sk,
                                        branch_desc, cd_sp, st_appl, no_aggr,
                                                (CASE
                                            WHEN st_appl = 'OP' AND app_via = 'ACCMe' AND app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS ST_APPL_OP,
                                           (CASE
                                            WHEN st_appl = 'RE' AND app_via = 'ACCMe' AND app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS ST_APPL_RE,
                                        (CASE
                                            WHEN app_via = 'ACCMe' AND app_type = 'M'
                                               THEN COUNT (no_aggr)
                                            ELSE 0
                                         END
                                        ) AS TOTAL_AGGR,
                                        (CASE
                                            WHEN app_via = 'ACCMe' AND app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS st_appl,
                                        (CASE
                                            WHEN app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS total_approval,
                                        (CASE
                                            WHEN app_via = 'ACCMe'
                                            AND app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS accme,
                                        (CASE
                                            WHEN app_via = 'AOL'
                                            AND app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS aol
                                   FROM (SELECT DISTINCT * /*branch_sk, branch_desc,
                                                  cd_sp, target_area_desc,
                                                  app_via, dt_appl, app_type, st_appl,no_aggr*/
                                             FROM accdwh_udw.accme_app_det_fact
                                             WHERE (GROUP_USER = 'RRSH' OR ID_USER LIKE '%.B%') --SUW, RROH DAN OH--
                                             AND TO_NUMBER (TO_CHAR(dt_appl,'YYYYMMDD')) BETWEEN 
                                             substr(v_time,1,6)||'01' AND v_time
                                             AND branch_desc not like 'HRD%' 
                                             AND branch_desc not like 'FLEET%'
                                             AND branch_desc not like 'FIF%'
                                             AND branch_desc not like '%PRODA'
                                             AND branch_desc not like 'COMMERCIAL%'
                                             AND branch_desc not like 'CENTRALIZED SURVEY%'
                                             AND branch_desc not like 'PONTIANAK AVALIS'
                                             AND branch_desc not like 'HEAD OFFICE'
                                             AND branch_desc not like 'COLLECTION CENTRO%'
                                                              AND branch_desc IN (SELECT DISTINCT BRANCH_DESC
                                                        FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT
                                                        WHERE STATUS = 'Implement'
                                                        AND TIME_SK = (SELECT MAX(X.TIME_SK) 
                                                     FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT X))
                                         ORDER BY name_user)
                               GROUP BY target_area_desc,
                                        branch_sk,
                                        branch_desc,
                                        cd_sp,
                                        app_via,
                                        app_type,
                                        st_appl,
                                        no_aggr) bx
                     GROUP BY bx.target_area_desc,
                              bx.branch_sk,
                              bx.branch_desc,
                              bx.cd_sp
                     ORDER BY bx.target_area_desc,
                              bx.branch_sk,
                              bx.branch_desc,
                              bx.cd_sp) xx
      UNION ALL
      -- Approval Capacity Per SUW --
      SELECT xx.time_sk, xx.branch_sk, xx.time_upload, xx.target_area_desc,
             xx.branch_desc, xx.branch_code, xx.group_name, xx.person_name,
             xx.total_approval, xx.approval_aol, xx.approval_accme,
             xx.percentage_aol, xx.percentage_accme, xx.type_dashboard, xx.ST_APPL_OP, xx.ST_APPL_RE, xx.TOTAL_AGGR
        FROM (SELECT DISTINCT v_time AS time_sk, bx.branch_sk AS branch_sk,
                              CURRENT_DATE AS time_upload,
                              bx.target_area_desc AS target_area_desc,
                              bx.branch_desc AS branch_desc,
                              bx.cd_sp AS branch_code,
                              bx.group_user AS group_name,
                              bx.name_user AS person_name,
                              SUM (bx.total_approval) AS total_approval,
                              SUM (bx.aol) AS approval_aol,
                              SUM (bx.accme) AS approval_accme,
                                    SUM (bx.ST_APPL_OP) AS ST_APPL_OP,
                              SUM (bx.ST_APPL_RE) AS ST_APPL_RE,
                              SUM (bx.TOTAL_AGGR) AS TOTAL_AGGR,
                              DECODE
                                  (SUM (bx.total_approval),
                                   0, 0,
                                     (SUM (bx.aol) / SUM (bx.total_approval)
                                     )
                                   * 100
                                  ) AS percentage_aol,
                              DECODE
                                 (SUM (bx.total_approval),
                                  0, 0,
                                    (SUM (bx.accme) / SUM (bx.total_approval)
                                    )
                                  * 100
                                 ) AS percentage_accme,
                              'APP_CAP_SUW' AS type_dashboard
                         FROM (SELECT   target_area_desc, branch_sk,
                                        branch_desc, cd_sp, name_user,
                                        group_user, st_appl, no_aggr,
                                               (CASE
                                            WHEN st_appl = 'OP' AND app_via = 'ACCMe' AND app_type = 'C'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS ST_APPL_OP,
                                           (CASE
                                            WHEN st_appl = 'RE' AND app_via = 'ACCMe' AND app_type = 'C'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS ST_APPL_RE,
                                        (CASE
                                            WHEN app_via = 'ACCMe' AND app_type = 'C'
                                               THEN COUNT (no_aggr)
                                            ELSE 0
                                         END
                                        ) AS TOTAL_AGGR,
                                        (CASE
                                            WHEN app_via = 'ACCMe' AND app_type = 'C'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS st_appl,
                                        (CASE
                                            WHEN app_type = 'C'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS total_approval,
                                        (CASE
                                            WHEN app_via = 'ACCMe'
                                            AND app_type = 'C'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS accme,
                                        (CASE
                                            WHEN app_via = 'AOL'
                                            AND app_type = 'C'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS aol
                                   FROM (SELECT  target_area_desc, branch_sk,
                                                  branch_desc, cd_sp, app_via,
                                                  name_user, group_user,
                                                  dt_appl, app_type, st_appl, no_aggr
                                             FROM accdwh_udw.accme_app_det_fact
                                             WHERE GROUP_USER = 'SUW'  --SUW-
                                             AND TO_NUMBER (TO_CHAR(dt_appl,'YYYYMMDD')) BETWEEN 
                                             substr(v_time,1,6)||'01' AND v_time
                                             AND branch_desc not like 'HRD%' 
                                             AND branch_desc not like 'FLEET%'
                                             AND branch_desc not like 'FIF%'
                                             AND branch_desc not like '%PRODA'
                                             AND branch_desc not like 'COMMERCIAL%'
                                             AND branch_desc not like 'CENTRALIZED SURVEY%'
                                             AND branch_desc not like 'PONTIANAK AVALIS'
                                             AND branch_desc not like 'HEAD OFFICE'
                                             AND branch_desc not like 'COLLECTION CENTRO%'
                                                              AND branch_desc IN (SELECT DISTINCT BRANCH_DESC
                                                        FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT
                                                        WHERE STATUS = 'Implement'
                                                        AND TIME_SK = (SELECT MAX(X.TIME_SK) 
                                                     FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT X))
                                         ORDER BY name_user)
                               GROUP BY target_area_desc,
                                        branch_sk,
                                        branch_desc,
                                        cd_sp,
                                        app_via,
                                        app_type,
                                        name_user,
                                        group_user,
                                        st_appl,
                              no_aggr) bx
                        WHERE bx.group_user = 'SUW'
                     GROUP BY bx.target_area_desc,
                              bx.branch_sk,
                              bx.branch_desc,
                              bx.cd_sp,
                              bx.name_user,
                              bx.group_user
                     ORDER BY bx.target_area_desc,
                              bx.branch_sk,
                              bx.branch_desc,
                              bx.cd_sp) xx
/*UNION ALL
      -- Approval Marex Per SUW --
      SELECT xx.time_sk, xx.branch_sk, xx.time_upload, xx.target_area_desc,
             xx.branch_desc, xx.branch_code, xx.group_name, xx.person_name,
             xx.total_approval, xx.approval_aol, xx.approval_accme,
             xx.percentage_aol, xx.percentage_accme, xx.type_dashboard, xx.ST_APPL_OP, xx.ST_APPL_RE, xx.TOTAL_AGGR
        FROM (SELECT DISTINCT v_time AS time_sk, bx.branch_sk AS branch_sk,
                              CURRENT_DATE AS time_upload,
                              bx.target_area_desc AS target_area_desc,
                              bx.branch_desc AS branch_desc,
                              bx.cd_sp AS branch_code,
                              bx.group_user AS group_name,
                              bx.name_user AS person_name,
                              SUM (bx.total_approval) AS total_approval,
                              SUM (bx.aol) AS approval_aol,
                              SUM (bx.accme) AS approval_accme,
                                     SUM (bx.ST_APPL_OP) AS ST_APPL_OP,
                              SUM (bx.ST_APPL_RE) AS ST_APPL_RE,
                              SUM (bx.TOTAL_AGGR) AS TOTAL_AGGR,
                              DECODE
                                  (SUM (bx.total_approval),
                                   0, 0,
                                     (SUM (bx.aol) / SUM (bx.total_approval)
                                     )
                                   * 100
                                  ) AS percentage_aol,
                              DECODE
                                 (SUM (bx.total_approval),
                                  0, 0,
                                    (SUM (bx.accme) / SUM (bx.total_approval)
                                    )
                                  * 100
                                 ) AS percentage_accme,
                              'APP_MAR_SUW' AS type_dashboard
                         FROM (SELECT   target_area_desc, branch_sk,
                                        branch_desc, cd_sp, name_user,
                                        group_user, st_appl,
                                        no_aggr,
                                                (CASE
                                            WHEN st_appl = 'OP' AND app_via = 'ACCMe' AND app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS ST_APPL_OP,
                                           (CASE
                                            WHEN st_appl = 'RE' AND app_via = 'ACCMe' AND app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS ST_APPL_RE,
                                        (CASE
                                            WHEN app_via = 'ACCMe' AND app_type = 'M'
                                               THEN COUNT (no_aggr)
                                            ELSE 0
                                         END
                                        ) AS TOTAL_AGGR,
                                        (CASE
                                            WHEN app_via = 'ACCMe' AND app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS st_appl,
                                        (CASE
                                            WHEN app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS total_approval,
                                        (CASE
                                            WHEN app_via = 'ACCMe'
                                            AND app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS accme,
                                        (CASE
                                            WHEN app_via = 'AOL'
                                            AND app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS aol
                                   FROM (SELECT target_area_desc, branch_sk,
                                                  branch_desc, cd_sp, app_via,
                                                  name_user, group_user,
                                                  dt_appl, app_type, st_appl,no_aggr
                                             FROM accdwh_udw.accme_app_det_fact
                                             WHERE TO_NUMBER (TO_CHAR(dt_appl,'YYYYMMDD')) BETWEEN 
                                               --REPLACE(v_time, SUBSTR(TO_CHAR(v_time), -2, 2), '01') AND v_time
                                             substr(v_time,1,6)||'01' AND v_time
                                             AND branch_desc not like 'HRD%' 
                                             AND branch_desc not like 'FLEET%'
                                             AND branch_desc not like 'FIF%'
                                             AND branch_desc not like '%PRODA'
                                             AND branch_desc not like 'COMMERCIAL%'
                                             AND branch_desc not like 'CENTRALIZED SURVEY%'
                                             AND branch_desc not like 'PONTIANAK AVALIS'
                                             AND branch_desc not like 'HEAD OFFICE'
                                             AND branch_desc not like 'COLLECTION CENTRO%'
                                                              AND branch_desc IN (SELECT DISTINCT BRANCH_DESC
                                                        FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT
                                                        WHERE STATUS = 'Implement'
                                                        AND TIME_SK = (SELECT MAX(X.TIME_SK) 
                                                     FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT X))
                                         ORDER BY name_user)
                               GROUP BY target_area_desc,
                                        branch_sk,
                                        branch_desc,
                                        cd_sp,
                                        app_via,
                                        app_type,
                                        name_user,
                                        group_user,
                                        st_appl,
                              no_aggr) bx
                        WHERE bx.group_user = 'SUW'
                     GROUP BY bx.target_area_desc,
                              bx.branch_sk,
                              bx.branch_desc,
                              bx.cd_sp,
                              bx.name_user,
                              bx.group_user
                     ORDER BY bx.target_area_desc,
                              bx.branch_sk,
                              bx.branch_desc,
                              bx.cd_sp) xx*/
UNION ALL
      -- Approval Capacity Per RROH --
      SELECT xx.time_sk, xx.branch_sk, xx.time_upload, xx.target_area_desc,
             xx.branch_desc, xx.branch_code, xx.group_name, xx.person_name,
             xx.total_approval, xx.approval_aol, xx.approval_accme,
             xx.percentage_aol, xx.percentage_accme, xx.type_dashboard, xx.ST_APPL_OP, xx.ST_APPL_RE, xx.TOTAL_AGGR
        FROM (SELECT DISTINCT v_time AS time_sk, bx.branch_sk AS branch_sk,
                              CURRENT_DATE AS time_upload,
                              bx.target_area_desc AS target_area_desc,
                              bx.branch_desc AS branch_desc,
                              bx.cd_sp AS branch_code,
                              bx.group_user AS group_name,
                              bx.name_user AS person_name,
                              SUM (bx.total_approval) AS total_approval,
                              SUM (bx.aol) AS approval_aol,
                              SUM (bx.accme) AS approval_accme,
                                    SUM (bx.ST_APPL_OP) AS ST_APPL_OP,
                              SUM (bx.ST_APPL_RE) AS ST_APPL_RE,
                              SUM (bx.TOTAL_AGGR) AS TOTAL_AGGR,
                              DECODE
                                  (SUM (bx.total_approval),
                                   0, 0,
                                     (SUM (bx.aol) / SUM (bx.total_approval)
                                     )
                                   * 100
                                  ) AS percentage_aol,
                              DECODE
                                 (SUM (bx.total_approval),
                                  0, 0,
                                    (SUM (bx.accme) / SUM (bx.total_approval)
                                    )
                                  * 100
                                 ) AS percentage_accme,
                              'APP_CAP_RROH' AS type_dashboard
                         FROM (SELECT   target_area_desc, branch_sk,
                                        branch_desc, cd_sp, name_user,
                                        group_user, st_appl,no_aggr,
                                        (CASE
                                            WHEN st_appl = 'OP' AND app_via = 'ACCMe' AND app_type = 'C'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS ST_APPL_OP,
                                           (CASE
                                            WHEN st_appl = 'RE' AND app_via = 'ACCMe' AND app_type = 'C'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS ST_APPL_RE,
                                        (CASE
                                            WHEN app_via = 'ACCMe' AND app_type = 'C'
                                               THEN COUNT (no_aggr)
                                            ELSE 0
                                         END
                                        ) AS TOTAL_AGGR,
                                        (CASE
                                            WHEN app_via = 'ACCMe' AND app_type = 'C'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS st_appl,
                                        (CASE
                                            WHEN app_type = 'C'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS total_approval,
                                        (CASE
                                            WHEN app_via = 'ACCMe'
                                            AND app_type = 'C'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS accme,
                                        (CASE
                                            WHEN app_via = 'AOL'
                                            AND app_type = 'C'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS aol
                                   FROM (SELECT branch_sk, branch_desc,
                                                  cd_sp, target_area_desc,
                                                  app_via, name_user,
                                                  group_user, dt_appl,
                                                  app_type, st_appl, no_aggr
                                             FROM accdwh_udw.accme_app_det_fact
                                             WHERE 
                                             GROUP_USER IN ('UAC', 'RUWH')  --RROH--
                                             AND TO_NUMBER (TO_CHAR(dt_appl,'YYYYMMDD')) BETWEEN  
                                               --REPLACE(v_time, SUBSTR(TO_CHAR(v_time), -2, 2), '01') AND v_time
                                             substr(v_time,1,6)||'01' AND v_time
                                             AND branch_desc not like 'HRD%' 
                                             AND branch_desc not like 'FLEET%'
                                             AND branch_desc not like 'FIF%'
                                             AND branch_desc not like '%PRODA'
                                             AND branch_desc not like 'COMMERCIAL%'
                                             AND branch_desc not like 'CENTRALIZED SURVEY%'
                                             AND branch_desc not like 'PONTIANAK AVALIS'
                                             AND branch_desc not like 'HEAD OFFICE'
                                             AND branch_desc not like 'COLLECTION CENTRO%'
                                                              AND branch_desc IN (SELECT DISTINCT BRANCH_DESC
                                                        FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT
                                                        WHERE STATUS = 'Implement'
                                                        AND TIME_SK = (SELECT MAX(X.TIME_SK) 
                                                     FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT X))
                                         ORDER BY name_user)
                               GROUP BY target_area_desc,
                                        branch_sk,
                                        branch_desc,
                                        cd_sp,
                                        app_via,
                                        app_type,
                                        name_user,
                                        group_user,
                                        st_appl,
                              no_aggr) bx
                        --WHERE bx.group_user = 'RROH'
                     GROUP BY bx.target_area_desc,
                              bx.branch_sk,
                              bx.branch_desc,
                              bx.cd_sp,
                              bx.name_user,
                              bx.group_user
                     ORDER BY bx.target_area_desc,
                              bx.branch_sk,
                              bx.branch_desc,
                              bx.cd_sp) xx
/*UNION ALL
      -- Approval Marex Per BUH --
      SELECT xx.time_sk, xx.branch_sk, xx.time_upload, xx.target_area_desc,
             xx.branch_desc, xx.branch_code, xx.group_name, xx.person_name,
             xx.total_approval, xx.approval_aol, xx.approval_accme,
             xx.percentage_aol, xx.percentage_accme, xx.type_dashboard, xx.ST_APPL_OP, xx.ST_APPL_RE, xx.TOTAL_AGGR
        FROM (SELECT DISTINCT v_time AS time_sk, bx.branch_sk AS branch_sk,
                              CURRENT_DATE AS time_upload,
                              bx.target_area_desc AS target_area_desc,
                              bx.branch_desc AS branch_desc,
                              bx.cd_sp AS branch_code,
                              bx.group_user AS group_name,
                              bx.name_user AS person_name,
                              SUM (bx.total_approval) AS total_approval,
                              SUM (bx.aol) AS approval_aol,
                              SUM (bx.accme) AS approval_accme,
                                   SUM (bx.ST_APPL_OP) AS ST_APPL_OP,
                              SUM (bx.ST_APPL_RE) AS ST_APPL_RE,
                              SUM (bx.TOTAL_AGGR) AS TOTAL_AGGR,
                              DECODE
                                  (SUM (bx.total_approval),
                                   0, 0,
                                     (SUM (bx.aol) / SUM (bx.total_approval)
                                     )
                                   * 100
                                  ) AS percentage_aol,
                              DECODE
                                 (SUM (bx.total_approval),
                                  0, 0,
                                    (SUM (bx.accme) / SUM (bx.total_approval)
                                    )
                                  * 100
                                 ) AS percentage_accme,
                              'APP_MAR_BUH' AS type_dashboard
                         FROM (SELECT   target_area_desc, branch_sk,
                                        branch_desc, cd_sp, name_user,
                                        group_user, st_appl, no_aggr,
                                               (CASE
                                            WHEN st_appl = 'OP' AND app_via = 'ACCMe' AND app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS ST_APPL_OP,
                                           (CASE
                                            WHEN st_appl = 'RE' AND app_via = 'ACCMe' AND app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS ST_APPL_RE,
                                        (CASE
                                            WHEN app_via = 'ACCMe' AND app_type = 'M'
                                               THEN COUNT (no_aggr)
                                            ELSE 0
                                         END
                                        ) AS TOTAL_AGGR,
                                        (CASE
                                            WHEN app_via = 'ACCMe' AND app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS st_appl,
                                        (CASE
                                            WHEN app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS total_approval,
                                        (CASE
                                            WHEN app_via = 'ACCMe'
                                            AND app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS accme,
                                        (CASE
                                            WHEN app_via = 'AOL'
                                            AND app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS aol
                                   FROM (SELECT DISTINCT target_area_desc, branch_sk,
                                                  branch_desc, cd_sp, app_via,
                                                  name_user, group_user,
                                                  dt_appl, app_type, st_appl, no_aggr
                                             FROM accdwh_udw.accme_app_det_fact
                                             WHERE TO_NUMBER (TO_CHAR(dt_appl,'YYYYMMDD')) BETWEEN 
                                               --REPLACE(v_time, SUBSTR(TO_CHAR(v_time), -2, 2), '01') AND v_time
                                             substr(v_time,1,6)||'01' AND v_time
                                             AND branch_desc not like 'HRD%' 
                                             AND branch_desc not like 'FLEET%'
                                             AND branch_desc not like 'FIF%'
                                             AND branch_desc not like '%PRODA'
                                             AND branch_desc not like 'COMMERCIAL%'
                                             AND branch_desc not like 'CENTRALIZED SURVEY%'
                                             AND branch_desc not like 'PONTIANAK AVALIS'
                                             AND branch_desc not like 'HEAD OFFICE'
                                             AND branch_desc not like 'COLLECTION CENTRO%'
                                                              AND branch_desc IN (SELECT DISTINCT BRANCH_DESC
                                                        FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT
                                                        WHERE STATUS = 'Implement'
                                                        AND TIME_SK = (SELECT MAX(X.TIME_SK) 
                                                     FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT X))
                                         ORDER BY name_user)
                               GROUP BY target_area_desc,
                                        branch_sk,
                                        branch_desc,
                                        cd_sp,
                                        app_via,
                                        app_type,
                                        name_user,
                                        group_user,
                                        st_appl,
                                        no_aggr
                                        ) bx
                        WHERE bx.group_user = 'BUH'
                     GROUP BY bx.target_area_desc,
                              bx.branch_sk,
                              bx.branch_desc,
                              bx.cd_sp,
                              bx.name_user,
                              bx.group_user
                     ORDER BY bx.target_area_desc,
                              bx.branch_sk,
                              bx.branch_desc,
                              bx.cd_sp) xx
UNION ALL
      -- Approval Capacity Per RRSH / BM --
      SELECT xx.time_sk, xx.branch_sk, xx.time_upload, xx.target_area_desc,
             xx.branch_desc, xx.branch_code, xx.group_name, xx.person_name,
             xx.total_approval, xx.approval_aol, xx.approval_accme,
             xx.percentage_aol, xx.percentage_accme, xx.type_dashboard, xx.ST_APPL_OP, xx.ST_APPL_RE, xx.TOTAL_AGGR
        FROM (SELECT DISTINCT v_time AS time_sk, bx.branch_sk AS branch_sk,
                              CURRENT_DATE AS time_upload,
                              bx.target_area_desc AS target_area_desc,
                              bx.branch_desc AS branch_desc,
                              bx.cd_sp AS branch_code,
                              bx.group_user AS group_name,
                              bx.name_user AS person_name,
                              SUM (bx.total_approval) AS total_approval,
                              SUM (bx.aol) AS approval_aol,
                              SUM (bx.accme) AS approval_accme,
                                    SUM (bx.ST_APPL_OP) AS ST_APPL_OP,
                              SUM (bx.ST_APPL_RE) AS ST_APPL_RE,
                              SUM (bx.TOTAL_AGGR) AS TOTAL_AGGR,
                              DECODE
                                  (SUM (bx.total_approval),
                                   0, 0,
                                     (SUM (bx.aol) / SUM (bx.total_approval)
                                     )
                                   * 100
                                  ) AS percentage_aol,
                              DECODE
                                 (SUM (bx.total_approval),
                                  0, 0,
                                    (SUM (bx.accme) / SUM (bx.total_approval)
                                    )
                                  * 100
                                 ) AS percentage_accme,
                              'APP_CAP_RRSH' AS type_dashboard
                         FROM (SELECT   target_area_desc, branch_sk,
                                        branch_desc, cd_sp, name_user,
                                        group_user, st_appl,
                                        no_aggr,
                                         (CASE
                                            WHEN st_appl = 'OP' AND app_via = 'ACCMe' AND app_type = 'C'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS ST_APPL_OP,
                                           (CASE
                                            WHEN st_appl = 'RE' AND app_via = 'ACCMe' AND app_type = 'C'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS ST_APPL_RE,
                                        (CASE
                                            WHEN app_via = 'ACCMe' AND app_type = 'C'
                                               THEN COUNT (no_aggr)
                                            ELSE 0
                                         END
                                        ) AS TOTAL_AGGR,
                                        (CASE
                                            WHEN app_via = 'ACCMe' AND app_type = 'C'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS st_appl,
                                        (CASE
                                            WHEN app_type = 'C'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS total_approval,
                                        (CASE
                                            WHEN app_via = 'ACCMe'
                                            AND app_type = 'C'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS accme,
                                        (CASE
                                            WHEN app_via = 'AOL'
                                            AND app_type = 'C'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS aol
                                   FROM (SELECT branch_sk, branch_desc,
                                                  cd_sp, target_area_desc,
                                                  app_via, name_user,
                                                  group_user, dt_appl,
                                                  app_type, st_appl, no_aggr
                                             FROM accdwh_udw.accme_app_det_fact
                                             WHERE TO_NUMBER (TO_CHAR(dt_appl,'YYYYMMDD')) BETWEEN 
                                               --REPLACE(v_time, SUBSTR(TO_CHAR(v_time), -2, 2), '01') AND v_time
                                             substr(v_time,1,6)||'01' AND v_time
                                             AND branch_desc not like 'HRD%' 
                                             AND branch_desc not like 'FLEET%'
                                             AND branch_desc not like 'FIF%'
                                             AND branch_desc not like '%PRODA'
                                             AND branch_desc not like 'COMMERCIAL%'
                                             AND branch_desc not like 'CENTRALIZED SURVEY%'
                                             AND branch_desc not like 'PONTIANAK AVALIS'
                                             AND branch_desc not like 'HEAD OFFICE'
                                             AND branch_desc not like 'COLLECTION CENTRO%'
                                                              AND branch_desc IN (SELECT DISTINCT BRANCH_DESC
                                                        FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT
                                                        WHERE STATUS = 'Implement'
                                                        AND TIME_SK = (SELECT MAX(X.TIME_SK) 
                                                     FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT X))
                                         ORDER BY name_user)
                               GROUP BY target_area_desc,
                                        branch_sk,
                                        branch_desc,
                                        cd_sp,
                                        app_via,
                                        app_type,
                                        name_user,
                                        group_user,
                                        st_appl,
                                        no_aggr) bx
                        WHERE bx.group_user = 'BM'
                     GROUP BY bx.target_area_desc,
                              bx.branch_sk,
                              bx.branch_desc,
                              bx.cd_sp,
                              bx.name_user,
                              bx.group_user
                     ORDER BY bx.target_area_desc,
                              bx.branch_sk,
                              bx.branch_desc,
                              bx.cd_sp) xx*/
UNION ALL
      -- Approval Marex Per BM--
      SELECT xx.time_sk, xx.branch_sk, xx.time_upload, xx.target_area_desc,
             xx.branch_desc, xx.branch_code, xx.group_name, xx.person_name,
             xx.total_approval, xx.approval_aol, xx.approval_accme,
             xx.percentage_aol, xx.percentage_accme, xx.type_dashboard, xx.ST_APPL_OP, xx.ST_APPL_RE, xx.TOTAL_AGGR
        FROM (SELECT DISTINCT v_time AS time_sk, bx.branch_sk AS branch_sk,
                              CURRENT_DATE AS time_upload,
                              bx.target_area_desc AS target_area_desc,
                              bx.branch_desc AS branch_desc,
                              bx.cd_sp AS branch_code,
                              bx.group_user AS group_name,
                              bx.name_user AS person_name,
                              SUM (bx.total_approval) AS total_approval,
                              SUM (bx.aol) AS approval_aol,
                              SUM (bx.accme) AS approval_accme,
                                     SUM (bx.ST_APPL_OP) AS ST_APPL_OP,
                              SUM (bx.ST_APPL_RE) AS ST_APPL_RE,
                              SUM (bx.TOTAL_AGGR) AS TOTAL_AGGR,
                              DECODE
                                  (SUM (bx.total_approval),
                                   0, 0,
                                     (SUM (bx.aol) / SUM (bx.total_approval)
                                     )
                                   * 100
                                  ) AS percentage_aol,
                              DECODE
                                 (SUM (bx.total_approval),
                                  0, 0,
                                    (SUM (bx.accme) / SUM (bx.total_approval)
                                    )
                                  * 100
                                 ) AS percentage_accme,
                              'APP_MAR_BM' AS type_dashboard
                         FROM (SELECT   target_area_desc, branch_sk,
                                        branch_desc, cd_sp, name_user,
                                        group_user, st_appl,
                                        no_aggr,
                                                (CASE
                                            WHEN st_appl = 'OP' AND app_via = 'ACCMe' AND app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS ST_APPL_OP,
                                           (CASE
                                            WHEN st_appl = 'RE' AND app_via = 'ACCMe' AND app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS ST_APPL_RE,
                                        (CASE
                                            WHEN app_via = 'ACCMe' AND app_type = 'M'
                                               THEN COUNT (no_aggr)
                                            ELSE 0
                                         END
                                        ) AS TOTAL_AGGR,
                                        (CASE
                                            WHEN app_via = 'ACCMe' AND app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS st_appl,
                                        (CASE
                                            WHEN app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS total_approval,
                                        (CASE
                                            WHEN app_via = 'ACCMe'
                                            AND app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS accme,
                                        (CASE
                                            WHEN app_via = 'AOL'
                                            AND app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS aol
                                   FROM (SELECT target_area_desc, branch_sk,
                                                  branch_desc, cd_sp, app_via,
                                                  name_user, id_user, group_user,
                                                  dt_appl, app_type, st_appl,
                                                  no_aggr
                                             FROM accdwh_udw.accme_app_det_fact
                                             WHERE ID_USER LIKE '%.B%' -- BM --
                                             AND TO_NUMBER (TO_CHAR(dt_appl,'YYYYMMDD')) BETWEEN 
                                             substr(v_time,1,6)||'01' AND v_time
                                             AND branch_desc not like 'HRD%' 
                                             AND branch_desc not like 'FLEET%'
                                             AND branch_desc not like 'FIF%'
                                             AND branch_desc not like '%PRODA'
                                             AND branch_desc not like 'COMMERCIAL%'
                                             AND branch_desc not like 'CENTRALIZED SURVEY%'
                                             AND branch_desc not like 'PONTIANAK AVALIS'
                                             AND branch_desc not like 'HEAD OFFICE'
                                             AND branch_desc not like 'COLLECTION CENTRO%'
                                                              AND branch_desc IN (SELECT DISTINCT BRANCH_DESC
                                                        FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT
                                                        WHERE STATUS = 'Implement'
                                                        AND TIME_SK = (SELECT MAX(X.TIME_SK) 
                                                     FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT X))
                                         ORDER BY name_user)
                               GROUP BY target_area_desc,
                                        branch_sk,
                                        branch_desc,
                                        cd_sp,
                                        app_via,
                                        app_type,
                                        name_user,
                                        group_user,
                                        st_appl,
                                        no_aggr) bx
                        --WHERE bx.group_user = 'BM'
                     GROUP BY bx.target_area_desc,
                              bx.branch_sk,
                              bx.branch_desc,
                              bx.cd_sp,
                              bx.name_user,
                              bx.group_user
                     ORDER BY bx.target_area_desc,
                              bx.branch_sk,
                              bx.branch_desc,
                              bx.cd_sp) xx
UNION ALL
      -- Approval Capacity Per OH --
      SELECT xx.time_sk, xx.branch_sk, xx.time_upload, xx.target_area_desc,
             xx.branch_desc, xx.branch_code, xx.group_name, xx.person_name,
             xx.total_approval, xx.approval_aol, xx.approval_accme,
             xx.percentage_aol, xx.percentage_accme, xx.type_dashboard, xx.ST_APPL_OP, xx.ST_APPL_RE, xx.TOTAL_AGGR
        FROM (SELECT DISTINCT v_time AS time_sk, bx.branch_sk AS branch_sk,
                              CURRENT_DATE AS time_upload,
                              bx.target_area_desc AS target_area_desc,
                              bx.branch_desc AS branch_desc,
                              bx.cd_sp AS branch_code,
                              bx.group_user AS group_name,
                              bx.name_user AS person_name,
                              SUM (bx.total_approval) AS total_approval,
                              SUM (bx.aol) AS approval_aol,
                              SUM (bx.accme) AS approval_accme,
                                    SUM (bx.ST_APPL_OP) AS ST_APPL_OP,
                              SUM (bx.ST_APPL_RE) AS ST_APPL_RE,
                              SUM (bx.TOTAL_AGGR) AS TOTAL_AGGR,
                              DECODE
                                  (SUM (bx.total_approval),
                                   0, 0,
                                     (SUM (bx.aol) / SUM (bx.total_approval)
                                     )
                                   * 100
                                  ) AS percentage_aol,
                              DECODE
                                 (SUM (bx.total_approval),
                                  0, 0,
                                    (SUM (bx.accme) / SUM (bx.total_approval)
                                    )
                                  * 100
                                 ) AS percentage_accme,
                              'APP_CAP_OH' AS type_dashboard
                         FROM (SELECT   target_area_desc, branch_sk,
                                        branch_desc, cd_sp, name_user,
                                        group_user, st_appl,
                                        no_aggr,
                                        (CASE
                                            WHEN st_appl = 'OP' AND app_via = 'ACCMe' AND app_type = 'C'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS ST_APPL_OP,
                                           (CASE
                                            WHEN st_appl = 'RE' AND app_via = 'ACCMe' AND app_type = 'C'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS ST_APPL_RE,
                                        (CASE
                                            WHEN app_via = 'ACCMe' AND app_type = 'C'
                                               THEN COUNT (no_aggr)
                                            ELSE 0
                                         END
                                        ) AS TOTAL_AGGR,
                                        (CASE
                                            WHEN app_via = 'ACCMe' AND app_type = 'C'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS st_appl,
                                        (CASE
                                            WHEN app_type = 'C'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS total_approval,
                                        (CASE
                                            WHEN app_via = 'ACCMe'
                                            AND app_type = 'C'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS accme,
                                        (CASE
                                            WHEN app_via = 'AOL'
                                            AND app_type = 'C'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS aol
                                   FROM (SELECT branch_sk, branch_desc,
                                                  cd_sp, target_area_desc,
                                                  app_via, name_user,
                                                  id_user, group_user, dt_appl,
                                                  app_type, st_appl,
                                                  no_aggr
                                             FROM accdwh_udw.accme_app_det_fact
                                             WHERE 
                                             ID_USER LIKE '%.H%' -- OH --
                                             AND TO_NUMBER (TO_CHAR(dt_appl,'YYYYMMDD')) BETWEEN substr(v_time,1,6)||'01' AND v_time
                                             AND branch_desc not like 'HRD%' 
                                             AND branch_desc not like 'FLEET%'
                                             AND branch_desc not like 'FIF%'
                                             AND branch_desc not like '%PRODA'
                                             AND branch_desc not like 'COMMERCIAL%'
                                             AND branch_desc not like 'CENTRALIZED SURVEY%'
                                             AND branch_desc not like 'PONTIANAK AVALIS'
                                             AND branch_desc not like 'HEAD OFFICE'
                                             AND branch_desc not like 'COLLECTION CENTRO%'
                                                              AND branch_desc IN (SELECT DISTINCT BRANCH_DESC
                                                        FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT
                                                        WHERE STATUS = 'Implement'
                                                        AND TIME_SK = (SELECT MAX(X.TIME_SK) 
                                                     FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT X))
                                         ORDER BY name_user)
                               GROUP BY target_area_desc,
                                        branch_sk,
                                        branch_desc,
                                        cd_sp,
                                        app_via,
                                        app_type,
                                        name_user,
                                        group_user,
                                        st_appl,
                                        no_aggr) bx
                        --WHERE bx.group_user = 'OH'
                     GROUP BY bx.target_area_desc,
                              bx.branch_sk,
                              bx.branch_desc,
                              bx.cd_sp,
                              bx.name_user,
                              bx.group_user
                     ORDER BY bx.target_area_desc,
                              bx.branch_sk,
                              bx.branch_desc,
                              bx.cd_sp) xx
/*UNION ALL
      -- Approval Marex Per OH --
      SELECT xx.time_sk, xx.branch_sk, xx.time_upload, xx.target_area_desc,
             xx.branch_desc, xx.branch_code, xx.group_name, xx.person_name,
             xx.total_approval, xx.approval_aol, xx.approval_accme,
             xx.percentage_aol, xx.percentage_accme, xx.type_dashboard, xx.ST_APPL_OP, xx.ST_APPL_RE, xx.TOTAL_AGGR
        FROM (SELECT DISTINCT v_time AS time_sk, bx.branch_sk AS branch_sk,
                              CURRENT_DATE AS time_upload,
                              bx.target_area_desc AS target_area_desc,
                              bx.branch_desc AS branch_desc,
                              bx.cd_sp AS branch_code,
                              bx.group_user AS group_name,
                              bx.name_user AS person_name,
                              SUM (bx.total_approval) AS total_approval,
                              SUM (bx.aol) AS approval_aol,
                              SUM (bx.accme) AS approval_accme,
                                   SUM (bx.ST_APPL_OP) AS ST_APPL_OP,
                              SUM (bx.ST_APPL_RE) AS ST_APPL_RE,
                              SUM (bx.TOTAL_AGGR) AS TOTAL_AGGR,
                              DECODE
                                  (SUM (bx.total_approval),
                                   0, 0,
                                     (SUM (bx.aol) / SUM (bx.total_approval)
                                     )
                                   * 100
                                  ) AS percentage_aol,
                              DECODE
                                 (SUM (bx.total_approval),
                                  0, 0,
                                    (SUM (bx.accme) / SUM (bx.total_approval)
                                    )
                                  * 100
                                 ) AS percentage_accme,
                              'APP_MAR_OH' AS type_dashboard
                         FROM (SELECT   target_area_desc, branch_sk,
                                        branch_desc, cd_sp, name_user,
                                        group_user, st_appl, no_aggr,
                                        (CASE
                                            WHEN st_appl = 'OP' AND app_via = 'ACCMe' AND app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS ST_APPL_OP,
                                           (CASE
                                            WHEN st_appl = 'RE' AND app_via = 'ACCMe' AND app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS ST_APPL_RE,
                                        (CASE
                                            WHEN app_via = 'ACCMe' AND app_type = 'M'
                                               THEN COUNT (no_aggr)
                                            ELSE 0
                                         END
                                        ) AS TOTAL_AGGR,
                                        (CASE
                                            WHEN app_via = 'ACCMe' AND app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS st_appl,
                                        (CASE
                                            WHEN app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS total_approval,
                                        (CASE
                                            WHEN app_via = 'ACCMe'
                                            AND app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS accme,
                                        (CASE
                                            WHEN app_via = 'AOL'
                                            AND app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS aol
                                   FROM (SELECT branch_sk, branch_desc,
                                                  cd_sp, target_area_desc,
                                                  app_via, name_user,
                                                  group_user, dt_appl,
                                                  app_type, st_appl, no_aggr
                                             FROM accdwh_udw.accme_app_det_fact
                                             WHERE TO_NUMBER (TO_CHAR(dt_appl,'YYYYMMDD')) BETWEEN 
                                               --REPLACE(v_time, SUBSTR(TO_CHAR(v_time), -2, 2), '01') AND v_time
                                             substr(v_time,1,6)||'01' AND v_time
                                             AND branch_desc not like 'HRD%' 
                                             AND branch_desc not like 'FLEET%'
                                             AND branch_desc not like 'FIF%'
                                             AND branch_desc not like '%PRODA'
                                             AND branch_desc not like 'COMMERCIAL%'
                                             AND branch_desc not like 'CENTRALIZED SURVEY%'
                                             AND branch_desc not like 'PONTIANAK AVALIS'
                                             AND branch_desc not like 'HEAD OFFICE'
                                             AND branch_desc not like 'COLLECTION CENTRO%'
                                                              AND branch_desc IN (SELECT DISTINCT BRANCH_DESC
                                                        FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT
                                                        WHERE STATUS = 'Implement'
                                                        AND TIME_SK = (SELECT MAX(X.TIME_SK) 
                                                     FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT X))
                                         ORDER BY name_user)
                               GROUP BY target_area_desc,
                                        branch_sk,
                                        branch_desc,
                                        cd_sp,
                                        app_via,
                                        app_type,
                                        name_user,
                                        group_user,
                                        st_appl,
                                        no_aggr) bx
                        WHERE bx.group_user = 'OH'
                     GROUP BY bx.target_area_desc,
                              bx.branch_sk,
                              bx.branch_desc,
                              bx.cd_sp,
                              bx.name_user,
                              bx.group_user
                     ORDER BY bx.target_area_desc,
                              bx.branch_sk,
                              bx.branch_desc,
                              bx.cd_sp) xx*/
UNION ALL
      -- Approval Marex Per RRSH --
      SELECT xx.time_sk, xx.branch_sk, xx.time_upload, xx.target_area_desc,
             xx.branch_desc, xx.branch_code, xx.group_name, xx.person_name,
             xx.total_approval, xx.approval_aol, xx.approval_accme,
             xx.percentage_aol, xx.percentage_accme, xx.type_dashboard, xx.ST_APPL_OP, xx.ST_APPL_RE, xx.TOTAL_AGGR
        FROM (SELECT DISTINCT v_time AS time_sk, bx.branch_sk AS branch_sk,
                              CURRENT_DATE AS time_upload,
                              bx.target_area_desc AS target_area_desc,
                              bx.branch_desc AS branch_desc,
                              bx.cd_sp AS branch_code,
                              bx.group_user AS group_name,
                              bx.name_user AS person_name,
                              SUM (bx.total_approval) AS total_approval,
                              SUM (bx.aol) AS approval_aol,
                              SUM (bx.accme) AS approval_accme,
                                   SUM (bx.ST_APPL_OP) AS ST_APPL_OP,
                              SUM (bx.ST_APPL_RE) AS ST_APPL_RE,
                              SUM (bx.TOTAL_AGGR) AS TOTAL_AGGR,
                              DECODE
                                  (SUM (bx.total_approval),
                                   0, 0,
                                     (SUM (bx.aol) / SUM (bx.total_approval)
                                     )
                                   * 100
                                  ) AS percentage_aol,
                              DECODE
                                 (SUM (bx.total_approval),
                                  0, 0,
                                    (SUM (bx.accme) / SUM (bx.total_approval)
                                    )
                                  * 100
                                 ) AS percentage_accme,
                              'APP_MAR_RRSH' AS type_dashboard
                         FROM (SELECT   target_area_desc, branch_sk,
                                        branch_desc, cd_sp, name_user,
                                        group_user, st_appl, no_aggr,
                                        (CASE
                                            WHEN st_appl = 'OP' AND app_via = 'ACCMe' AND app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS ST_APPL_OP,
                                           (CASE
                                            WHEN st_appl = 'RE' AND app_via = 'ACCMe' AND app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS ST_APPL_RE,
                                        (CASE
                                            WHEN app_via = 'ACCMe' AND app_type = 'M'
                                               THEN COUNT (no_aggr)
                                            ELSE 0
                                         END
                                        ) AS TOTAL_AGGR,
                                        (CASE
                                            WHEN app_via = 'ACCMe' AND app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS st_appl,
                                        (CASE
                                            WHEN app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS total_approval,
                                        (CASE
                                            WHEN app_via = 'ACCMe'
                                            AND app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS accme,
                                        (CASE
                                            WHEN app_via = 'AOL'
                                            AND app_type = 'M'
                                               THEN COUNT (branch_desc)
                                            ELSE 0
                                         END
                                        ) AS aol
                                   FROM (SELECT branch_sk, branch_desc,
                                                  cd_sp, target_area_desc,
                                                  app_via, name_user,
                                                  group_user, dt_appl,
                                                  app_type, st_appl, no_aggr
                                             FROM accdwh_udw.accme_app_det_fact
                                             WHERE GROUP_USER = 'RRSH' --SUW, RROH DAN OH--
                                             AND TO_NUMBER (TO_CHAR(dt_appl,'YYYYMMDD')) BETWEEN  substr(v_time,1,6)||'01' AND v_time
                                             AND branch_desc not like 'HRD%' 
                                             AND branch_desc not like 'FLEET%'
                                             AND branch_desc not like 'FIF%'
                                             AND branch_desc not like '%PRODA'
                                             AND branch_desc not like 'COMMERCIAL%'
                                             AND branch_desc not like 'CENTRALIZED SURVEY%'
                                             AND branch_desc not like 'PONTIANAK AVALIS'
                                             AND branch_desc not like 'HEAD OFFICE'
                                             AND branch_desc not like 'COLLECTION CENTRO%'
                                                              AND branch_desc IN (SELECT DISTINCT BRANCH_DESC
                                                        FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT
                                                        WHERE STATUS = 'Implement'
                                                        AND TIME_SK = (SELECT MAX(X.TIME_SK) 
                                                     FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT X))
                                         ORDER BY name_user)
                               GROUP BY target_area_desc,
                                        branch_sk,
                                        branch_desc,
                                        cd_sp,
                                        app_via,
                                        app_type,
                                        name_user,
                                        group_user,
                                        st_appl,
                                        no_aggr) bx
                        --WHERE bx.group_user = 'RRSDH'
                     GROUP BY bx.target_area_desc,
                              bx.branch_sk,
                              bx.branch_desc,
                              bx.cd_sp,
                              bx.name_user,
                              bx.group_user
                     ORDER BY bx.target_area_desc,
                              bx.branch_sk,
                              bx.branch_desc,
                              bx.cd_sp) xx;
      
BEGIN
   COMMIT;

   FOR c1rec IN c1
   LOOP
      FOR c2rec IN c2 (c1rec.v_time, c1rec.v_month, c1rec.v_year)
      LOOP
         INSERT INTO accdwh_udw.accme_approval_fact
                     (time_sk, branch_sk, time_upload,
                      target_area_desc, branch_desc,
                      branch_code, group_name,
                      person_name, total_approval,
                      approval_aol, approval_accme,
                      percentage_aol, percentage_accme,
                      type_dashboard, st_appl_op, st_appl_re, total_no_aggr, status
                     )
              VALUES (c2rec.time_sk, c2rec.branch_sk, c2rec.time_upload,
                      c2rec.target_area_desc, c2rec.branch_desc,
                      c2rec.branch_code, c2rec.group_name,
                      c2rec.person_name, c2rec.total_approval,
                      c2rec.approval_aol, c2rec.approval_accme,
                      c2rec.percentage_aol, c2rec.percentage_accme,
                      c2rec.type_dashboard, c2rec.st_appl_op, c2rec.st_appl_re, c2rec.total_aggr, null
                     );

         COMMIT;
      END LOOP;
   END LOOP;
   
    UPDATE ACCDWH_UDW.ACCME_APPROVAL_FACT Y
      SET Y.STATUS = 'Implement'
    WHERE Y.BRANCH_CODE IN (SELECT DISTINCT CD_SP 
                             FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT
                            WHERE STATUS = 'Implement'
                              AND TIME_SK = (SELECT MAX(X.TIME_SK) 
                                               FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT X));

    UPDATE ACCDWH_UDW.ACCME_APPROVAL_FACT Y
      SET Y.STATUS = 'Implement'
    WHERE Y.TYPE_DASHBOARD like 'APP%AREA' 
      AND Y.TARGET_AREA_DESC IN ( SELECT AREA 
                            FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT
                            WHERE STATUS = 'Implement'
                              AND TIME_SK = (SELECT MAX(X.TIME_SK) 
                                               FROM ACCDWH_UDW.ACCME_BRANCH_DET_FACT X) group by AREA);

   COMMIT;
   


   
END;
/
