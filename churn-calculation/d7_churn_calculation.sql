-- Databricks notebook source
-- MAGIC %md
-- MAGIC Select churn_d07 by level for the last 30 days

-- COMMAND ----------

--CREATE WIDGET TEXT dateToProcess DEFAULT '2022-08-07'

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dateToProcess AS
SELECT (CASE WHEN '$dateToProcess' <> 'null' THEN DATE('$dateToProcess') ELSE DATE_ADD(CURRENT_DATE, -1) END) AS date_to_process
;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v0_rounds_raw AS
  SELECT
  /* +broadcast(room) */
    round_start_dt,
    round_result_cd,
    a.account_id,
    a.application_cd,
    a.application_family_name,
    a.market_cd,
    room_stc.location AS room_location,
    room_stc.name AS room_name,
    CASE 
      WHEN room.order_num IS NOT NULL THEN room.order_num
      ELSE room_stc.order
    END AS room_order,
    CASE 
      WHEN room.order_num IS NULL THEN 'Side Content'
      WHEN room.order_num BETWEEN 1 AND 100000 THEN 'Main Content'
      WHEN room.order_num BETWEEN 100001 AND 200000 THEN 'EOC'
      ELSE 'Side Content'
    END AS main_content_ind,
    payer_ind,
    application_version_val,
    CASE 
      WHEN UPPER(a.application_family_name) = 'PANDA POP'
      THEN round_end_map.bubbles_rem
      ELSE round_moves_stc.remaining 
    END AS moves_remaining_group,
    ROW_NUMBER() OVER(PARTITION BY a.application_cd, account_id, round_start_dt ORDER BY round_start_dttm DESC) AS rounds_order
  FROM pr_analytics_delta.round_event a
  LEFT JOIN pr_analytics_lkp.room room
  ON upper(a.room_stc.name) = upper(room.room_name)
    AND coalesce(upper(a.room_stc.location),'') = coalesce(upper(room.location_name),'')
    AND a.application_cd = room.application_cd
  WHERE round_start_dt BETWEEN DATE_ADD((SELECT date_to_process FROM dateToProcess), -21) AND (SELECT date_to_process FROM dateToProcess)
    AND UPPER(a.application_family_name) IN ('COOKIE JAM', 'COOKIE JAM BLAST', 'PANDA POP', 'GENIES AND GEMS', 'FROZEN FREE FALL', 'FAMILY GUY')
;

CREATE OR REPLACE TEMP VIEW v0_last_activity AS
  SELECT
    round_start_dt,
    account_id,
    round_result_cd,
    application_cd,
    application_family_name,
    market_cd,
    room_location,
    room_name,
    room_order,
    main_content_ind,
    payer_ind,
    application_version_val,
    CASE
      WHEN moves_remaining_group IS NULL THEN 'Not Informed'
      WHEN moves_remaining_group = 0 THEN '0'
      WHEN moves_remaining_group = 1 THEN '1'
      WHEN moves_remaining_group = 2 THEN '2'
      WHEN moves_remaining_group = 3 THEN '3'
      WHEN moves_remaining_group IN (4, 5) THEN '4-5'
      WHEN moves_remaining_group IN (6, 7, 8, 9, 10) THEN '6-10'
      ELSE '11+'
    END AS moves_remaining_group,
    LEAD(round_start_dt) OVER (PARTITION BY application_cd, account_id ORDER BY round_start_dt ASC) AS next_round_start_dt
  FROM v1_rounds_raw
  WHERE rounds_order = 1
;

CREATE OR REPLACE TEMP VIEW v1_last_activity AS
  SELECT
    *,
    DATEDIFF(CASE 
              WHEN next_round_start_dt IS NULL THEN DATE_ADD((SELECT date_to_process FROM dateToProcess), 1) 
              ELSE next_round_start_dt 
            END, round_start_dt
    ) AS days_between
  FROM v0_last_activity
;

CREATE OR REPLACE TEMP VIEW v0_d7_churned AS
  SELECT
    *,
    CASE WHEN days_between > 07 THEN 1 ELSE 0 END AS churned_d07
  FROM v1_last_activity
;

CREATE OR REPLACE TEMP VIEW churn_final AS
SELECT
  (SELECT date_to_process FROM dateToProcess) AS date_to_process,
  round_start_dt,
  application_cd,
  application_family_name,
  market_cd,
  room_location,
  room_name,
  room_order,
  main_content_ind,
  SUBSTRING_INDEX(application_version_val, '.', 2) as short_app_version_val,
  round_result_cd,
  moves_remaining_group,
  payer_ind,
  COUNT(DISTINCT account_id) AS dau,
  SUM(churned_d07) AS d7_churned
FROM v0_d7_churned
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC deltaLocation = 'dbfs:/mnt/jc-analytics-databricks-work/dv_analytics_adhoc/match_3_level_dashboard_churn'
-- MAGIC deltaDf = spark.sql("SELECT * FROM churn_final")
-- MAGIC (deltaDf
-- MAGIC   .write
-- MAGIC   .format("delta")
-- MAGIC   .mode("overwrite")
-- MAGIC   .option("overwriteSchema", True)
-- MAGIC   .save(deltaLocation)
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC createDelta = "CREATE TABLE IF NOT EXISTS dv_analytics_adhoc.match_3_level_dashboard_churn USING delta LOCATION 'dbfs:/mnt/jc-analytics-databricks-work/dv_analytics_adhoc/match_3_level_dashboard_churn'"
-- MAGIC spark.sql(createDelta)

-- COMMAND ----------

--OPTIMIZE dv_analytics_adhoc.atribec_147_churn;
--VACUUM dv_analytics_adhoc.atribec_147_churn;

-- COMMAND ----------

SELECT * FROM dv_analytics_adhoc.match_3_level_dashboard_churn

-- COMMAND ----------

SELECT
  date_to_process,
  round_start_dt,
  SUM(dau),
  SUM(d7_churned),
  SUM(d7_churned) / SUM(dau)
FROM dv_analytics_adhoc.match_3_level_dashboard_churn
GROUP BY 1,2

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
