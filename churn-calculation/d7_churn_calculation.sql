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
-- Extract all rounds played in the last 21 days and sort them from the most recent to the oldest.
CREATE OR REPLACE TEMP VIEW v0_rounds_raw AS  
  SELECT
  /* +broadcast(room) */
    round_start_dt,
    round_result_cd,
    a.account_id,
    a.application_cd,
    a.application_family,
    a.market_cd,
    room_stc.location AS room_location,
    room_stc.name AS room_name,
    CASE 
      WHEN room.order_num IS NOT NULL THEN room.order_num
      ELSE room_stc.order
    END AS room_order,
    CASE 
      WHEN room.order_num BETWEEN 1 AND 100000 THEN 'Main Content'
      ELSE 'Side Content'
    END AS main_content_ind,
    payer_ind,
    application_version_val,
    moves_remaining_group,
    ROW_NUMBER() OVER(PARTITION BY a.application_cd, account_id, round_start_dt ORDER BY round_start_dttm DESC) AS rounds_order
  FROM round_event_raw a
  LEFT JOIN room_lkp room
  ON upper(a.room_stc.name) = upper(room.room_name)
    AND coalesce(upper(a.room_stc.location),'') = coalesce(upper(room.location_name),'')
    AND a.application_cd = room.application_cd
  WHERE round_start_dt BETWEEN DATE_ADD((SELECT date_to_process FROM dateToProcess), -21) AND (SELECT date_to_process FROM dateToProcess)
    AND UPPER(a.application_family) IN ('GAME 1', 'GAME 2', 'GAME 3', 'GAME 4')
;

CREATE OR REPLACE TEMP VIEW v0_last_activity AS
-- Evaluate the first round of each day and obtain the next day played in each case.
  SELECT
    round_start_dt,
    account_id,
    round_result_cd,
    application_cd,
    application_family,
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
-- Get the difference in days between the current and the next round. If the player has not played since the last time, the current day -1 is used.
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
-- If more than 7 days have passed since the last time the player has played, the player is classified as churned.
  SELECT
    *,
    CASE WHEN days_between > 07 THEN 1 ELSE 0 END AS churned_d07
  FROM v1_last_activity
;

CREATE OR REPLACE TEMP VIEW churn_final AS
-- Aggregate data for easier analysis and export to Tableau.
SELECT
  (SELECT date_to_process FROM dateToProcess) AS date_to_process,
  round_start_dt,
  application_cd,
  application_family,
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
-- MAGIC deltaLocation = 'dbfs_location'
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
-- MAGIC createDelta = "CREATE TABLE IF NOT EXISTS sample_schema.d7_game_churn_analysis USING delta LOCATION 'dbfs_location'"
-- MAGIC spark.sql(createDelta)

-- COMMAND ----------

--OPTIMIZE sample_schema.d7_game_churn_analysis;
--VACUUM sample_schema.d7_game_churn_analysis;

-- COMMAND ----------
-- Just some control queries
SELECT * FROM sample_schema.d7_game_churn_analysis;

SELECT
  date_to_process,
  round_start_dt,
  SUM(dau),
  SUM(d7_churned),
  SUM(d7_churned) / SUM(dau)
FROM sample_schema.d7_game_churn_analysis
GROUP BY 1,2
;
