WITH grant_amounts AS (     -- CTE finds the information for each grant associated with each publication
  SELECT
    pubs.id,
    pubs.grants,
    ARRAY_LENGTH(gr.active_years) AS years_active,
    gr.funding_eur
  FROM (
    SELECT        -- unnesting the grants associated with each publication
      id,
      grants
    FROM `covid-19-dimensions-ai.data.publications`,
    UNNEST(supporting_grant_ids) AS grants) pubs
  JOIN `covid-19-dimensions-ai.data.grants` gr
  ON pubs.grants = gr.id),

  publication_totals AS (     --CTE calculates aggregates for each publication
  SELECT
    id,
    COUNT(grants) AS num_associated_grants,
    MAX(years_active) AS years_active,
    SUM(funding_eur) AS total_grant_amount
  FROM grant_amounts
  GROUP BY id
  ),

  patents AS (      --CTE finding the number and status of patents associated with each publication
    SELECT
      publication_ids,
      COUNTIF(patent_status = 'Active') AS num_active_patents,
      COUNTIF(patent_status = 'Pending') AS num_pending_patents,
      COUNT(patent_id) AS num_total_patents,
    FROM (
      SELECT
        publication_ids,
        id AS patent_id,
        legal_status,
        CASE        -- grouping patent statuses into more general groups
          WHEN legal_status IN ('Active', 'Granted') THEN 'Active'
          WHEN legal_status IN ('Pending', 'Published Application') THEN 'Pending'
          ELSE 'Patent unknown'
        END AS patent_status
      FROM `covid-19-dimensions-ai.data.patents`,
      UNNEST(publication_ids) AS publication_ids)
    GROUP BY publication_ids
    ),

    organisations AS (      --CTE finds number of different research organisations for each publication
      SELECT
        id,
        MAX(num_org_countries) AS num_org_countries,   --only 1 value to draw from
        COUNTIF(types = 'Nonprofit') AS num_nonprofit_orgs,
        COUNTIF(types = 'Company') AS num_company_orgs,
        COUNTIF(types = 'Education') AS num_education_orgs,
        COUNTIF(types = 'Facility') AS num_facility_orgs,
        COUNTIF(types = 'Government') AS num_government_orgs,
        COUNTIF(types = 'Healthcare') AS num_healthcare_orgs,
        COUNTIF(types = 'Archive') AS num_archive_orgs,
        COUNTIF(types = 'Other') As num_other_orgs
      FROM (
        SELECT        -- unnests the research organisations for each publication, then matches the id to the name/type
          pubs.id,
          ARRAY_LENGTH(research_org_countries) AS num_org_countries,
          research_orgs,
          grid.name,
          grid.types
        FROM `covid-19-dimensions-ai.data.publications` pubs,
        UNNEST(research_orgs) AS research_orgs
        JOIN `covid-19-dimensions-ai.data.grid` grid
        ON research_orgs = grid.id),
        UNNEST(types) AS types
        GROUP BY id
    ),

    ML_concepts AS (      --CTE finds the average relevance score of all publications relating to Machine Learning.
      SELECT
        id,
        AVG(score) AS avg_ML_score
      FROM (
        SELECT
          id,
          con.concept AS concept,
          con.relevance AS score,
          FROM `covid-19-dimensions-ai.data.publications`,
          UNNEST(concepts) AS con
          WHERE concepts IS NOT NULL
          AND concept IN ('ML', 'AI', 'machine learning', 'artifical intelligence', 'neural network', 'NLP', 'natural language processing', 'regression', 'linear regression', 'logisitic regression', 'decision tree', 'classifier', 'computer vision', 'clustering', 'supervised learning', 'unsupervised learning', 'random forest', 'deep learning', 'artificial neural networks', 'convolutional neural network', 'support vector machine', 'naive bayes', 'generative AI')
      )
      GROUP BY id
      ),
    
    journal_scores AS(
      SELECT    --turning this into an overall journal score at time of publication
        *,
        total_altm_at_time_of_pub/num_publications AS journal_impact_score
      FROM(
        SELECT    -- finding the running total of publications and altmetric scores for each journal
          *,
          COUNT(id) OVER (PARTITION BY journal_id ORDER BY date_published) AS num_publications, 
          SUM(altmetric_score) OVER (PARTITION BY journal_id ORDER BY date_published) AS total_altm_at_time_of_pub,
        FROM (
          SELECT  -- finding altmetric scores for each publication
            journal.id AS journal_id,
            id,
            CASE
              WHEN LENGTH(date) = 7 THEN PARSE_DATE('%Y-%m-%d', CONCAT(date, '-01'))
              WHEN LENGTH(date) = 10 THEN PARSE_DATE('%Y-%m-%d', date)
              ELSE NULL
            END AS date_published,
            altmetrics.score AS altmetric_score
          FROM `covid-19-dimensions-ai.data.publications`))
    ),

    author_scores AS (
      SELECT      -- totals the number of previous publications/altmetric score for each publication
        id,
        COUNT(DISTINCT researcher_id) AS num_researchers,
        SUM(num_prev_pubs) AS total_prev_pubs,
        SUM(sum_altmetrics) AS total_prev_altmetrics
      FROM (
        SELECT      -- finds number of prvious publications and their altmetric scores for each author
          *,
          COUNT(id) OVER (PARTITION BY researcher_id ORDER BY publication_date) AS num_prev_pubs,
          SUM(altmetric_score) OVER (PARTITION BY researcher_id ORDER BY publication_date) AS sum_altmetrics
        FROM (
          SELECT      -- unnests authors for each publication
            auth.researcher_id,
            id,
            CASE
                  WHEN LENGTH(date) = 7 THEN PARSE_DATE('%Y-%m-%d', CONCAT(date, '-01'))
                  WHEN LENGTH(date) = 10 THEN PARSE_DATE('%Y-%m-%d', date)
                  ELSE NULL
                END AS publication_date,
            altmetrics.score AS altmetric_score
          FROM `covid-19-dimensions-ai.data.publications`,
          UNNEST(authors) AS auth
          WHERE altmetrics.score IS NOT NULL AND researcher_id IS NOT NULL))
        GROUP BY id
    )


SELECT        -- Final Select Statement for regression analysis
  pubs.id,
  altmetrics.score AS alt_score,      --target variable
  ARRAY_LENGTH(pubs.research_orgs) AS num_research_orgs,
  ARRAY_LENGTH(patent_ids) AS num_patents,
  COALESCE(citations_count, 0) AS num_times_cited,
  ARRAY_LENGTH(researcher_ids) AS num_researchers,
  metrics.recent_citations,
  COALESCE(ml.avg_ML_score, 0) AS ML_relevance,
  --(SELECT STRING_AGG(con.concept, ', ') FROM UNNEST(concepts) AS con) AS concepts,    --converting array into string of concepts
  type AS publication_type,
  date,
  ARRAY_LENGTH(clinical_trial_ids) AS num_clinical_trials,
  ptot.num_associated_grants,
  ptot.years_active,
  ptot.total_grant_amount,
  pat.num_active_patents,
  pat.num_pending_patents,
  orgs.* EXCEPT(id),          -- adding all columns from organisations CTE except the id column
  js.num_publications AS num_prev_pubs_by_journal,
  js.total_altm_at_time_of_pub AS total_journal_altmetric,
  js.journal_impact_score,
  auths.total_prev_pubs,
  auths.total_prev_altmetrics,
  auths.total_prev_pubs / ARRAY_LENGTH(researcher_ids) AS prev_pubs_per_auths,
  auths.total_prev_altmetrics / ARRAY_LENGTH(researcher_ids) AS prev_altmetrics_per_auths
FROM `covid-19-dimensions-ai.data.publications` pubs
LEFT JOIN publication_totals ptot
ON ptot.id = pubs.id
LEFT JOIN patents pat
ON pat.publication_ids = pubs.id
LEFT JOIN organisations orgs
ON orgs.id = pubs.id
LEFT JOIN ML_concepts ml
ON ml.id = pubs.id
LEFT JOIN journal_scores js
ON pubs.id = js.id
LEFT JOIN author_scores auths
ON auths.id = pubs.id
WHERE altmetrics.score IS NOT NULL        -- filtering only for publications with altmetric score (target variable)
