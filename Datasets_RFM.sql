WITH ML_scores AS (
  SELECT      --CTE finds the relevance scores of all datasets related/using to Machine Learning/AI (how much that data involves ML)
    id,
    AVG(relevance) AS ML_relevance
  FROM (
    SELECT
      id,
      con.concept,
      con.relevance
    FROM `covid-19-dimensions-ai.data.datasets`,
    UNNEST(concepts) AS con
    WHERE concept IN ('ML', 'AI', 'machine learning', 'artifical intelligence', 'neural network', 'NLP', 'natural language processing', 'regression', 'linear regression', 'logisitic regression', 'decision tree', 'classifier', 'computer vision', 'clustering', 'supervised learning', 'unsupervised learning', 'random forest', 'deep learning', 'artificial neural networks', 'convolutional neural network', 'support vector machine', 'naive bayes', 'generative AI')
  )
  GROUP BY id
  ),

  datasets AS (
  SELECT      -- CTE collates the adjusted RFM scores (RIA...Relevance, Impact, Accessibilty)
    dset.title AS Dataset,
    COALESCE(ml.ML_relevance*100, 0) AS ML_relevance,
    pubs.altmetrics.score AS alt_metric_score,
    ARRAY_LENGTH(pubs.clinical_trial_ids) AS num_associated_clinical_trials,
    ARRAY_LENGTH(dset.associated_grant_ids) AS num_associated_grants,
    ARRAY_LENGTH(dset.research_org_countries) AS org_countries,
    license.name,
    dset.repository_url,
    pubs.category_hra.values AS category,
    dset.date AS publication_date,
    CASE        -- Ranking the usage licenses by increasing restrictions
      WHEN REPLACE(UPPER(license.name), '-', ' ') LIKE '%CC0%'
        OR REPLACE(UPPER(license.name), '-', ' ') LIKE 'CC PDDC'
        OR REPLACE(UPPER(license.name), '-', ' ') LIKE 'ODC%'
        OR REPLACE(UPPER(license.name), '-', ' ') LIKE 'PUBLIC DOMAIN' THEN 1
      WHEN REPLACE(UPPER(license.name), '-', ' ') LIKE 'CC BY _.0'
        OR REPLACE(UPPER(license.name), '-', ' ') LIKE 'CC BY'
        OR REPLACE(UPPER(license.name), '-', ' ') LIKE 'GPL%'
        OR REPLACE(UPPER(license.name), '-', ' ') LIKE 'OGL%'
        OR REPLACE(UPPER(license.name), '-', ' ') LIKE 'APACHE%'
        OR REPLACE(UPPER(license.name), '-', ' ') LIKE 'MIT' THEN 2
      WHEN REPLACE(UPPER(license.name), '-', ' ') LIKE 'CC BY SA _.0' THEN 3
      WHEN REPLACE(UPPER(license.name), '-', ' ') LIKE 'CC BY NC _.0'
        OR REPLACE(UPPER(license.name), '-', ' ') LIKE '%(CC BY NC)' THEN 4
      WHEN REPLACE(UPPER(license.name), '-', ' ') LIKE 'CC BY NC SA _.0' THEN 5
      WHEN REPLACE(UPPER(license.name), '-', ' ') LIKE 'CC BY ND _.0' THEN 6
      WHEN REPLACE(UPPER(license.name), '-', ' ') LIKE 'CC BY NC ND _.0' THEN 7
      WHEN REPLACE(UPPER(license.name), '-', ' ') IS NULL THEN NULL
      ELSE 8 END AS license_severity
  FROM `covid-19-dimensions-ai.data.datasets` dset
  LEFT JOIN ML_scores ml
  ON dset.id = ml.id
  LEFT JOIN `covid-19-dimensions-ai.data.publications` pubs
  ON dset.associated_publication_id = pubs.id
  WHERE license.name IS NOT NULL AND pubs.altmetrics.score IS NOT NULL),

  imp_percentiles AS (      --Finding percentiles of altmetrics to give score
    SELECT
      APPROX_QUANTILES(d.alt_metric_score, 100)[OFFSET(25)] AS imp_25th,
      APPROX_QUANTILES(d.alt_metric_score, 100)[OFFSET(50)] AS imp_50th,
      APPROX_QUANTILES(d.alt_metric_score, 100)[OFFSET(75)] AS imp_75th,
    FROM datasets d
  ),

  rel_percentiles AS (      --Finding percentiles of ML relevance, only based on datsets related to ML.
    SELECT
      APPROX_QUANTILES(d.ML_relevance, 100)[OFFSET(25)] AS rel_25th,
      APPROX_QUANTILES(d.ML_relevance, 100)[OFFSET(50)] AS rel_50th,
      APPROX_QUANTILES(d.ML_relevance, 100)[OFFSET(75)] AS rel_75th,
    FROM datasets d
    WHERE d.ML_relevance > 0

  ),

  scores AS (
  SELECT
    *,
    CAST(ROUND((rel_score+imp_score)/2) AS INT64) AS rel_imp_combo    -- combining relavence and impact scores
  FROM (
    SELECT          --Scoring the datasets, creating RIA scores. 1 being the worst, 4 being the best.
      *,
      CASE
        WHEN ML_relevance < rel_pct.rel_25th THEN 1
        WHEN ML_relevance >= rel_pct.rel_25th AND ML_relevance < rel_pct.rel_50th THEN 2
        WHEN ML_relevance >= rel_pct.rel_50th AND ML_relevance < rel_pct.rel_75th THEN 3
        WHEN ML_relevance >= rel_pct.rel_75th THEN 4
      END AS rel_score,
      CASE
        WHEN alt_metric_score < imp_25th THEN 1
        WHEN alt_metric_score >= imp_25th AND alt_metric_score < imp_50th THEN 2
        WHEN alt_metric_score >= imp_50th AND alt_metric_score < imp_75th THEN 3
        WHEN alt_metric_score >= imp_75th THEN 4
      END AS imp_score,
      CASE
        WHEN license_severity <= 2 THEN 4   --reversing the scores so 4 is the 'least restrictive'
        WHEN license_severity > 2 AND license_severity <= 4 THEN 3
        WHEN license_severity > 4 AND license_severity <= 6 THEN 2
        WHEN license_severity > 6 THEN 1
      END AS acc_score,
    FROM datasets dset, rel_percentiles rel_pct, imp_percentiles imp_pct))

SELECT      -- Creating the segments based on their RIA scores
  Dataset,
  ML_relevance,
  alt_metric_score,
  license_severity,
  COALESCE(category, 'Not specified') AS category,
  num_associated_clinical_trials,
  num_associated_grants,
  org_countries,
  DATE(publication_date) AS publication_date,
  CASE
    WHEN rel_imp_combo = 4 AND (acc_score = 4 OR acc_score = 3) THEN 'Gold star'
    WHEN rel_imp_combo = 3 AND (acc_score = 4 OR acc_score = 3) THEN 'Silver star'
    WHEN (rel_imp_combo = 4 OR rel_imp_combo = 3) AND (acc_score < 3) THEN 'Difficult to access'
    WHEN rel_imp_combo < 3 AND (acc_score = 4 OR acc_score = 3) THEN 'Accessible but irrelavent'
    WHEN rel_imp_combo < 3 AND acc_score = 2 THEN 'Not very promising'
    WHEN rel_imp_combo < 3 AND acc_score = 1 THEN 'Not worthwhile'
  END AS segment,
  repository_url
FROM scores,
UNNEST(category) AS category
