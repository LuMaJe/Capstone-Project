WITH pat_codes AS (     --CTE finds the (majority) patent type code for each patent
  SELECT
    id,
    type_code
  FROM (
    SELECT    -- ranking the codes for each patent (based on most common) in the case of multiple types
      *,
      ROW_NUMBER() OVER (PARTITION BY id ORDER BY num_codes DESC) AS code_rank
    FROM (
      SELECT
        id,     -- taking the counts of each type of patent code (based on first letter of the code)
        LEFT(cpc,1) AS type_code,
        COUNT(LEFT(cpc,1)) AS num_codes
      FROM`covid-19-dimensions-ai.data.patents`,
      UNNEST(cpc) AS cpc
      GROUP BY id, type_code)
  )
  WHERE code_rank = 1   -- majority only
  ),

  ML_scores AS (
    SELECT        --CTE finds the relevance scores for all patents related to ML
      id,
      AVG(relevance) AS ML_score
    FROM (
      SELECT
        id,
        con.concept,
        con.relevance
      FROM`covid-19-dimensions-ai.data.patents`,
      UNNEST(concepts) AS con
      WHERE con.concept IN ('ML', 'AI', 'machine learning', 'artifical intelligence', 'neural network', 'NLP', 'natural language processing', 'regression', 'linear regression', 'logisitic regression', 'decision tree', 'classifier', 'computer vision', 'clustering', 'supervised learning', 'unsupervised learning', 'random forest', 'deep learning', 'artificial neural networks', 'convolutional neural network', 'support vector machine', 'naive bayes', 'generative AI'))
    GROUP BY id
  ),

  cohorts AS (
  SELECT      -- Final table showing patents, information on coverage/types and expiration dates
    pats.id,
    legal_status,
    title,
    (SELECT STRING_AGG(inventor_names, ', ') FROM UNNEST(inventor_names) AS inventor_names) AS inventors,
    (SELECT STRING_AGG(ca.name, ', ') FROM UNNEST(current_assignee) AS ca) AS current_assignees,    --listing inventors/asignees from array
    ARRAY_LENGTH(funder_orgs) AS num_funder_orgs,
    granted_date,
    DATE(expiration_date) AS expiration_date,
    DATE_DIFF(DATE(expiration_date), DATE(granted_date), YEAR) AS patent_length_years,
    COALESCE(ml.ML_score, 0) AS ML_score,   
    CASE WHEN ml.ML_score IS NOT NULL THEN True ELSE False END AS contains_ML,    --if the patent contains ML content or not
    CASE
      WHEN jurisdiction LIKE 'WO' THEN 'Worldwide'
      WHEN jurisdiction LIKE 'CN' THEN 'China'
      WHEN jurisdiction LIKE 'EP' THEN 'Europe'
      WHEN jurisdiction LIKE 'US' THEN 'US'
      WHEN jurisdiction LIKE 'KR' THEN 'S. Korea'
      ELSE 'Other'
    END AS jurisdiction,      --grouping into the most common groups and other
    CASE          -- translating patent codes to their subject names
      WHEN pc.type_code = 'A' THEN 'Human Necessities'
      WHEN pc.type_code = 'B' THEN 'Performing Operations'
      WHEN pc.type_code = 'C' THEN 'Chemistry / Metallurgy'
      WHEN pc.type_code = 'D' THEN 'Textiles'
      WHEN pc.type_code = 'E' THEN 'Fixed Constructions'
      WHEN pc.type_code = 'F' THEN 'Mechanical Engineering'
      WHEN pc.type_code = 'G' THEN 'Physics'
      WHEN pc.type_code = 'H' THEN 'Electricity'
      WHEN pc.type_code = 'Y' THEN 'Cross-sectional'
    END AS patent_type,
    claims_amount AS num_patent_claims,
    IF(DATE(granted_date) <= '2023-12-31' AND DATE(expiration_date) > '2023-12-31', 1, 0) AS active_2023,   -- allowing for cohort tallies.
    IF(DATE(granted_date) <= '2024-12-31' AND DATE(expiration_date) > '2024-12-31', 1, 0) AS active_2024,
    IF(DATE(granted_date) <= '2025-12-31' AND DATE(expiration_date) > '2025-12-31', 1, 0) AS active_2025,
    IF(DATE(granted_date) <= '2026-12-31' AND DATE(expiration_date) > '2026-12-31', 1, 0) AS active_2026,
    IF(DATE(granted_date) <= '2027-12-31' AND DATE(expiration_date) > '2027-12-31', 1, 0) AS active_2027,
    IF(DATE(granted_date) <= '2028-12-31' AND DATE(expiration_date) > '2028-12-31', 1, 0) AS active_2028,
    IF(DATE(granted_date) <= '2029-12-31' AND DATE(expiration_date) > '2029-12-31', 1, 0) AS active_2029,
    IF(DATE(granted_date) <= '2030-12-31' AND DATE(expiration_date) > '2030-12-31', 1, 0) AS active_2030,
    IF(DATE(granted_date) <= '2031-12-31' AND DATE(expiration_date) > '2031-12-31', 1, 0) AS active_2031,
    IF(DATE(granted_date) <= '2032-12-31' AND DATE(expiration_date) > '2032-12-31', 1, 0) AS active_2032,
    IF(DATE(granted_date) <= '2033-12-31' AND DATE(expiration_date) > '2033-12-31', 1, 0) AS active_2033,
  FROM `covid-19-dimensions-ai.data.patents` pats
  LEFT JOIN pat_codes pc
  ON pats.id = pc.id
  LEFT JOIN ML_scores ml
  ON pats.id = ml.id
  WHERE granted_date IS NOT NULL 
  AND expiration_date IS NOT NULL 
  AND pc.type_code IS NOT NULL)

SELECT      -- creating retention rates for each patent type based on the aggregates made in subquery below
  co.*,
  1-((total_2023-total_2023)/total_2023) AS retention_2023,
  1-((total_2023-total_2024)/total_2023) AS retention_2024,
  1-((total_2023-total_2025)/total_2023) AS retention_2025,
  1-((total_2023-total_2026)/total_2023) AS retention_2026,
  1-((total_2023-total_2027)/total_2023) AS retention_2027,
  1-((total_2023-total_2028)/total_2023) AS retention_2028,
  1-((total_2023-total_2029)/total_2023) AS retention_2029,
  1-((total_2023-total_2030)/total_2023) AS retention_2030,
  1-((total_2023-total_2031)/total_2023) AS retention_2031,
  1-((total_2023-total_2032)/total_2023) AS retention_2032,
  1-((total_2023-total_2033)/total_2023) AS retention_2033
FROM(
  SELECT
    patent_type,
    SUM(active_2023) AS total_2023,
    SUM(active_2024) AS total_2024,
    SUM(active_2025) AS total_2025,
    SUM(active_2026) AS total_2026,
    SUM(active_2027) AS total_2027,
    SUM(active_2028) AS total_2028,
    SUM(active_2029) AS total_2029,
    SUM(active_2030) AS total_2030,
    SUM(active_2031) AS total_2031,
    SUM(active_2032) AS total_2032,
    SUM(active_2033) AS total_2033
  FROM cohorts
  GROUP BY patent_type) co
