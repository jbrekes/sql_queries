WITH blacklist AS (
  SELECT
    account_id
  FROM analytics.blacklist
  WHERE UPPER(application_family) LIKE 'MAHJONG' 
), 

balance_list_raw AS
(
SELECT
    /*+ broadcast(bl) */
    ac.ACTIVITY_DT                                     AS activity_dt
  , ACCOUNT_ID                                          AS account_id
  , PAYER_IND                                           AS payer_ind
  , APPLICATION_FAMILY_NAME                             AS application_family
  , APPLICATION_VERSION_VAL                             AS application_version_val
  , MARKET_CD                                           AS platform
  , COUNTRY_STC.Name                                    AS country_name
  , MAX_LEVEL_CD                                        AS max_level_cd
  , ROUND(LIFETIME_GROSS_REVENUE, 2)                    AS lifetime_iap_gros_rev_amt
  , EXPLODE(BALANCE_LST)                                AS balanceJson
FROM
  analytics.account_activity ac
LEFT ANTI JOIN blacklist bl
  ON ac.account_id = bl.account_id
WHERE UPPER(APPLICATION_FAMILY_NAME) = 'GAME 1'
  AND ACTIVITY_DT >= CURRENT_DATE - 90
),

balance_list AS
(
SELECT
    account_id                                  AS account_id
  , activity_dt                                 AS activity_dt
  , payer_ind                                   AS payer_ind
  , lifetime_iap_gros_rev_amt                   AS lifetime_iap_gros_rev_amt
  , application_family                          AS application_family
  , application_version_val                     AS application_version_val
  , platform                                    AS platform
  , country_name                                AS country_name
  , max_level_cd                                AS max_level_cd
  , balanceJson.group.name                      AS group_name
  , balanceJson.group.type                      AS type
  , balanceJson.group.usage                     AS usage
  , balanceJson.balance                         AS balance
  , TO_DATE(balanceJson.dateTime, 'yyyy-MM-dd') AS balance_date
FROM
  balance_list_raw
)

SELECT
    activity_dt
  , account_id                                                                            AS account_id  
  , CASE 
      WHEN payer_ind = 0 THEN 'Non-Payer'
      WHEN payer_ind = 1 THEN 'Payer' 
    END                                                                                   AS payer_desc
  , CASE
      WHEN lifetime_iap_gros_rev_amt >= 500 THEN 1
      ELSE 0
    END                                                                                   AS vip_nd 
  , application_family                                                                    AS application_family
  , application_version_val                                                               AS application_version_val
  , platform                                                                              AS  market_cd
  , country_name                                                                          AS country_name
  , CASE 
      WHEN max_level_cd <= 0 THEN 'No Level'
      WHEN max_level_cd >= 1 AND max_level_cd <= 60 THEN 'Levels 1 - 60'
      WHEN max_level_cd >= 61 AND max_level_cd <= 120 THEN 'Levels 61 - 120'
      WHEN max_level_cd >= 121 AND max_level_cd <= 180 THEN 'Levels 121 - 180'
      WHEN max_level_cd >= 181 AND max_level_cd <= 500 THEN 'Levels 181 - 500'
      WHEN max_level_cd >= 501 AND max_level_cd <= 1000 THEN 'Levels 501 - 1000'
      WHEN max_level_cd >= 1001 AND max_level_cd <= 2000 THEN 'Levels 1001 - 2000'
      WHEN max_level_cd >= 2001 AND max_level_cd <= 3000 THEN 'Levels 2001 - 3000'
      WHEN max_level_cd >= 3001 AND max_level_cd <= 4000 THEN 'Levels 3001 - 4000'
      WHEN max_level_cd >= 4001 AND max_level_cd <= 5000 THEN 'Levels 4001 - 5000'
      WHEN max_level_cd >= 5001 THEN 'Levels 5000+'
      ELSE 'Others'
    END                                                                                   AS level_bucket_desc
  , group_name                                                                            AS resource_group_name
  , type                                                                                  AS resource_group_type_name
  , usage                                                                                 AS usage_desc
  , balance                                                                               AS balance_qty
FROM
  balance_list
WHERE
  balance_date = activity_dt