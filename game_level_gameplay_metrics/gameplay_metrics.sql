-- Databricks notebook source
--CREATE WIDGET TEXT dateToProcess  DEFAULT 'null';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Game Level Dashboard - Gameplay Metrics
-- MAGIC
-- MAGIC [document for requirements](link_to_document.com)
-- MAGIC
-- MAGIC [Quadrant + Wireframe](link_to_mode_documentation.com)
-- MAGIC

-- COMMAND ----------
-- Select the date and games to be processed
CREATE OR REPLACE TEMP VIEW dates AS
SELECT (CASE WHEN '$dateToProcess' <> 'null' THEN DATE('$dateToProcess') ELSE DATE_ADD(CURRENT_DATE, -1) END) AS process_dt
;

CREATE OR REPLACE TEMP VIEW vo_apps_WR AS
SELECT DISTINCT application_cd
FROM game_schema.games_lkp
WHERE UPPER(application_family_name) IN ('GAME 1', 'GAME 2', 'GAME 3', 'GAME 4', 'GAME 5')
;

-- COMMAND ----------

-- Create a view with all the different items available in the games and their respective classification. 
-- Each game has different nomenclatures and rules so the standardization of the groups is sought.

CREATE OR REPLACE TEMPORARY VIEW v0_application_resource AS
SELECT
  APPLICATION_CD,
  UPPER(TRIM(A.RESOURCE_CD)) AS RESOURCE_CD,
  UPPER(TRIM(A.RESOURCE_GROUP_TYPE_CD)) AS RESOURCE_GROUP_TYPE_CD
FROM game_schema.resources
;

CREATE OR REPLACE TEMPORARY VIEW v0_virtual_item_full AS 
SELECT  
  APPLICATION_CD,
  RESOURCE_CD,
  CASE
      WHEN APPLICATION_CD IN (1111,2222,3333,4444) AND (RESOURCE_CD LIKE '%PLUS%MOVE%') -- Fake IDs, just for privacy
      THEN 'EXTRA MOVES'
      ELSE RESOURCE_GROUP_TYPE_CD
  END RESOURCE_GROUP_TYPE_CD,
FROM v0_application_resource
WHERE RESOURCE_GROUP_TYPE_CD IS NOT NULL

UNION ALL

SELECT
  APPLICATION_CD,
  RESOURCE_CD AS RESOURCE_CD,
  CASE WHEN RESOURCE_CD LIKE '%EXTRA%MOVE%'
                  OR RESOURCE_CD LIKE '%KEEP%PLAY%'
                  OR RESOURCE_CD LIKE '%EXTRA%BU%'
                  OR RESOURCE_CD LIKE '%MOVES%'
                  THEN 'EXTRA_MOVE'
        WHEN RESOURCE_CD LIKE '%LIFE%'
                  OR RESOURCE_CD LIKE '%LIVES%' THEN 'LIVE'
        WHEN RESOURCE_CD IS NOT NULL
                  AND RESOURCE_CD NOT LIKE '%EXTRA%MOVE%'
                  AND RESOURCE_CD NOT LIKE '%KEEP%PLAY%'
                  AND RESOURCE_CD NOT LIKE '%EXTRA%BU%'
                  AND RESOURCE_CD NOT LIKE '%MOVES%'
                  AND RESOURCE_CD NOT LIKE '%LIFE%'
                  AND RESOURCE_CD NOT LIKE '%LIVES%'
                  AND RESOURCE_CD NOT LIKE '%HEART%'
        THEN 'POWER_UP' 
        ELSE 'OTHERS' END  AS    RESOURCE_GROUP_TYPE_CD
FROM v0_application_resource
WHERE RESOURCE_GROUP_TYPE_CD IS NULL
;


CREATE OR REPLACE TEMPORARY VIEW v1_virtual_item_full AS
SELECT 
  APPLICATION_CD,
  RESOURCE_CD,
  RESOURCE_GROUP_TYPE_CD
FROM (
    SELECT 
      APPLICATION_CD,
      RESOURCE_CD,
      RESOURCE_GROUP_TYPE_CD,
      ROW_NUMBER() OVER (PARTITION BY APPLICATION_CD, RESOURCE_CD ORDER BY ORDEN) Q_VAL
    FROM v0_virtual_item_full
    ) B
WHERE Q_VAL = 1
;

-- COMMAND ----------
-- VIRTUAL ITEMS USED
-- Analyze the use of virtual currency in games based on the activity of the corresponding dateToProcess.
CREATE OR REPLACE TEMP VIEW vo_game_vc_usage AS
SELECT
/*+broadcast(app)*/
  a.APPLICATION_CD,
  ACCOUNT_ID,  
  activity_dt,
  b.col.action,
  b.col.dateTime AS operation_moment,
  CASE
    WHEN FILTER(b.col.actions,element -> element.credits>0 AND element.group.usage = 'IMMEDIATE')[0] IS NULL
      THEN FILTER(b.col.actions,element -> element.debits>0 AND element.credits=0 ).resourceId
    ELSE FILTER(b.col.actions,element -> element.credits>0 ).resourceId
  END AS debited_resource_real
FROM games_schema.game_activity a
INNER JOIN vo_apps_WR app 
  ON a.application_cd = app.application_cd
LATERAL VIEW EXPLODE(resource_action_lst) b
WHERE  activity_dt = (SELECT process_dt FROM dates)
  AND UPPER(b.col.action) IN ('BUYWITHGAMECURRENCY', 'USE')
;

CREATE OR REPLACE TEMP VIEW v1_game_vc_usage AS
SELECT
/*+broadcast(b)*/
  a.APPLICATION_CD,
  a.ACCOUNT_ID,
  a.ACTIVITY_DT,
  a.ACTION,
  a.OPERATION_MOMENT,
  a.debited_resource_col,
  b.RESOURCE_CD,
  b.RESOURCE_GROUP_TYPE_CD
FROM
(
  SELECT
    a.*,
    trim(upper(debited_resource_COL))debited_resource_col
  FROM vo_game_vc_usage
  LATERAL VIEW EXPLODE(debited_resource_real) B AS debited_resource_COL
  WHERE trim(upper(debited_resource_COL)) <> 'COINS'
) a
INNER JOIN V1_VIRTUAL_ITEM_FULL b
ON a.debited_resource_col = UPPER(TRIM(b.RESOURCE_CD))
  AND a.application_Cd = b.application_Cd
;

-- COMMAND ----------

-- ROUNDS PLAYED
-- Obtain gameplay data for each game for the date analyzed (rounds played along with other useful data).

CREATE OR REPLACE TEMP VIEW vo_rounds_order AS
SELECT 
/*+broadcast(app)*/
  a.application_cd,
  a.application_family_name,
  a.market_cd,
  round_start_dt,
  round_start_dttm,
  round_end_dttm,
  ROW_NUMBER () OVER(PARTITION BY a.application_cd, account_id, session_uuid ORDER BY round_start_dttm ASC) AS daily_round_number,
  account_id,
  payer_ind,
  round_uuid,
  session_uuid,
  room_stc.location AS room_location,
  room_stc.name AS room_name,
  CASE 
    WHEN room.order_num IS NOT NULL THEN room.order_num
    ELSE room_stc.order
  END AS room_order,
  CASE 
    WHEN room.order_num IS NULL THEN 'Side Content'
    WHEN room.order_num BETWEEN 1 AND 100000 THEN 'Main Content'
    WHEN room.order_num >= 100001 THEN 'Side Content'
  END AS main_content_ind,
  round_result_cd,
  CASE 
    WHEN round_result_cd = 'win' AND attempt_num < 200 THEN attempt_num 
    ELSE 0 
  END AS attempts_to_win, -- Remove Outliers, there are cases with 3000 + attempts, wich is unreal
  round_moves_stc.remaining AS moves_remaining_group,
  round_moves_stc.used AS moves_used,
  CASE
    WHEN UPPER(a.application_family_name) = 'GAME 1'
      THEN round_end_map.explosiveness
    WHEN UPPER(a.application_family_name) = 'GAME 2'
      THEN AGGREGATE(TRANSFORM(SPLIT(TRIM('[]' FROM round_end_map.exploded),','), x -> CAST(x AS INT)), 0, (acc, x) -> acc + x)
    WHEN UPPER(a.application_family_name) = 'GAME 3'
      THEN AGGREGATE(TRANSFORM(SPLIT(TRIM('[]' FROM round_end_map.items_cleared_per_move),','), x -> CAST(x AS INT)), 0, (acc, x) -> acc + x)
    WHEN UPPER(a.application_family_name) = 'GAME 4'
      THEN round_end_map.pieces_cleared
    ELSE null
  END AS items_cleared,
  COALESCE(round_end_map.shuffle, 0) AS reshuffle,
  CASE WHEN round_end_map.avg_fps IS NULL THEN round_end_map.fps ELSE round_end_map.avg_fps END AS mean_fps,
  ROUND((round_seconds_qty/1000) / 60, 3) AS round_duration_min    
FROM events_schema.rounds a
INNER JOIN vo_apps_WR app 
  ON a.application_cd = app.application_cd
LEFT JOIN levels_lkp room
  ON upper(a.room_stc.name) = upper(room.room_name)
  AND COALESCE(UPPER(a.room_stc.location),'') = COALESCE(UPPER(room.location_name),'')
  AND a.application_cd = room.application_cd
WHERE round_start_dt = (SELECT process_dt FROM dates)
;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC deltaLocation = 'aux_gameplay_table_location'
-- MAGIC deltaDf = spark.sql("SELECT * FROM vo_rounds_order")
-- MAGIC (deltaDf
-- MAGIC   .write
-- MAGIC   .format("delta")
-- MAGIC   .mode("overwrite")
-- MAGIC   .option("overwriteSchema", True)
-- MAGIC   .save(deltaLocation)
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC createDelta = "CREATE TABLE IF NOT EXISTS temp_schemas.aux_rounds_order USING delta LOCATION 'aux_gameplay_table_location'"
-- MAGIC spark.sql(createDelta)

-- COMMAND ----------

-- CURRENT AND PREVIOUS ROUND

CREATE OR REPLACE TEMP VIEW vo_rounds_1 AS
SELECT
    a.*,
    LAG(round_start_dttm) OVER(PARTITION BY application_cd, account_id ORDER BY round_start_dttm DESC)
    LAG(round_end_dttm) OVER(PARTITION BY application_cd, account_id ORDER BY round_start_dttm DESC)
FROM temp_schemas.aux_rounds_order
;

-- COMMAND ----------

-- SESSIONS

CREATE OR REPLACE TEMP VIEW vo_sessions AS
SELECT
/*+broadcast(app)*/
  account_id,
  session_dt,
  a.application_family_name,
  session_uuid,
  session_start_dttm,
  session_end_dttm
FROM games_schema.game_activity a
INNER JOIN vo_apps_WR app 
  ON a.application_cd = app.application_cd
WHERE session_dt = (SELECT process_dt FROM dates)
;

-- COMMAND ----------

-- ROUNDS BELONGING TO EACH SESSION

CREATE OR REPLACE TEMP VIEW vo_rounds_2 AS
SELECT
  rounds.*,
  sessions.session_start_dttm,
  sessions.session_end_dttm
FROM vo_rounds_1 as rounds
INNER JOIN vo_sessions as sessions
ON UPPER(rounds.application_family_name) = UPPER(sessions.application_family_name)
  AND rounds.round_start_dt = sessions.session_dt
  AND rounds.account_id = sessions.account_id
  AND rounds.session_uuid = sessions.session_uuid
;

-- COMMAND ----------

-- GET THE MOMMENT EACH VIRTUAL ITEM WAS USED IN THE SESSION
-- We need to know if the user used an item while playing a round, or if it was consumed at another time (for example, in the start menu or in an additional event).

CREATE OR REPLACE TEMP VIEW vo_item_round AS
SELECT
  a.*,
  b.OPERATION_MOMENT,
  b.debited_resource_col,
  b.RESOURCE_CD,
  b.RESOURCE_GROUP_TYPE_CD,
  CASE
    WHEN operation_moment > round_start_dttm AND operation_moment < round_end_dttm
    THEN 1 -- 'IN LEVEL'
    WHEN operation_moment >= session_start_dttm 
      AND operation_moment <= round_start_dttm 
      AND (daily_round_number = 1
        OR operation_moment > previous_round_end_dttm
        OR (operation_moment > previous_round_start_dttm AND previous_round_end_dttm IS NULL))
    THEN 2 -- 'PRE LEVEL'
    ELSE 3 -- 'OTHER'
  END AS use_moment
FROM vo_rounds_2 a
LEFT JOIN vo_aaa_vc_usage b
ON a.application_cd = b.application_cd
  AND a.round_start_dt = b.activity_dt
  AND a.account_id = b.account_id
  AND operation_moment BETWEEN session_start_dttm AND session_end_dttm;
;
-- COMMAND ----------

-- KEEP ONLY RELEVANT RELATIONSHIPS

CREATE OR REPLACE TEMP VIEW vo_final AS
SELECT 
*
FROM
(
  SELECT
    application_cd,
    application_family_name,
    market_cd,
    round_start_dt,
    account_id,
    payer_ind,
    round_uuid,
    room_location,
    room_name,
    room_order,
    main_content_ind,
    round_result_cd,
    use_moment AS moment,
    ROW_NUMBER() OVER(PARTITION BY application_cd, account_id, round_uuid ORDER BY use_moment ASC) AS result,
    attempts_to_win,
    moves_remaining_group,
    moves_used,
    items_cleared,
    reshuffle,
    mean_fps,
    round_duration_min    
  FROM vo_item_round
)
WHERE result = 1
;  

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW vo_final_2 AS
SELECT
  application_cd,
  application_family_name,
  market_cd,
  round_start_dt,
  payer_ind,
  room_location,
  room_name,
  room_order,
  main_content_ind,
  round_result_cd,
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
  COUNT(CASE WHEN moment = 2 THEN 1 ELSE null END) AS pre_level_rounds,
  COUNT(CASE WHEN moment = 1 THEN 1 ELSE null END) AS in_level_rounds,
  COUNT(CASE WHEN moment NOT IN (1, 2) THEN 1 ELSE null END) AS other_rounds,
  COUNT(1) AS total_rounds,
  SUM(attempts_to_win) AS attempts_to_win,
  SUM(moves_used) AS moves_used,
  SUM(moves_remaining_group) AS moves_remaining,
  SUM(items_cleared) AS items_cleared,
  SUM(reshuffle) AS reshuffle,
  AVG(mean_fps) AS mean_fps,
  AVG(round_duration_min) AS round_duration_min
FROM vo_final
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
;

-- COMMAND ----------
-- The above information is recorded at user and level level, but the same user can play several levels in the same day. 
-- As required by the project, we need to obtain the DAU data for each game, regardless of how many levels have been played.

CREATE OR REPLACE TEMP VIEW vo_daily_players AS
SELECT 
/*+broadcast(app)*/
  a.application_cd,
  a.application_family_name,
  a.market_cd,
  round_start_dt,
  payer_ind,
  room_stc.location AS room_location,
  room_stc.name AS room_name,
  CASE 
    WHEN room.order_num IS NOT NULL THEN room.order_num
    ELSE room_stc.order
  END AS room_order,
  CASE 
    WHEN room.order_num IS NULL THEN 'Side Content'
    WHEN room.order_num BETWEEN 1 AND 100000 THEN 'Main Content'
    WHEN room.order_num >= 100001 THEN 'Side Content'
  END AS main_content_ind,
  COUNT(DISTINCT account_id) AS daily_level_players
FROM games_schema.game_activity a
INNER JOIN vo_apps_WR app 
  ON a.application_cd = app.application_cd
LEFT JOIN levels_lkp room
  ON upper(a.room_stc.name) = upper(room.room_name)
  AND coalesce(upper(a.room_stc.location),'') = coalesce(upper(room.location_name),'')
  AND a.application_cd = room.application_cd
WHERE round_start_dt = (SELECT process_dt FROM dates)
GROUP BY 1,2,3,4,5,6,7,8,9
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Combine Results

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW vo_final_3 AS
SELECT
  a.application_cd,
  a.application_family_name,
  a.market_cd,
  a.round_start_dt,
  a.payer_ind,
  a.room_location,
  a.room_name,
  a.room_order,
  a.main_content_ind,
  round_result_cd,
  moves_remaining_group,
  pre_level_rounds,
  in_level_rounds,
  other_rounds,
  total_rounds,
  attempts_to_win,
  moves_used,
  moves_remaining,
  items_cleared,
  reshuffle,
  mean_fps,
  round_duration_min,
  daily_level_players
FROM vo_final_2 a
INNER JOIN vo_daily_players b
ON a.application_cd = b.application_cd
  AND a.round_start_dt = b.round_start_dt
  AND a.payer_ind = b.payer_ind
  AND a.room_location = b.room_location
  AND a.room_name = b.room_name
  AND a.room_order = b.room_order
  AND a.main_content_ind = b.main_content_ind
;

-- COMMAND ----------

DELETE FROM custom_analysis_schema.gameplay_level_analysis
WHERE round_start_dt = (CASE WHEN '$dateToProcess' <> 'null' THEN '$dateToProcess' ELSE date_add(CURRENT_DATE, -1) END)
;

-- COMMAND ----------

INSERT INTO custom_analysis_schema.gameplay_level_analysis
SELECT * FROM vo_final_3
;