# Capstone-Project

## Project Description

### Overview
This project was inspired by a conversation with the CEO of a life-sciences company and replicated an analysis that would have been undertaken to solve the real-world problem. The project consists of 3 main parts, representing 3 unique problems for the company. This project involves modelling the impact of academic publications in order to strategise the companys publication output. Next, segmenting publicly available datasets  in order to improve their current machine learning capabilities. Finally, providing a FTO analysis regarding legal patents to ensure the avoidance of potential legal roadblocks.

### Project contents
* Data retrieval/analysis - SQL utilising Google BigQuery (see SQL queries in this repo)
  
* Regression model - Jupyter Notebooks (Python notebook available [here.](https://github.com/LuMaJe/Capstone-Project/blob/main/Linear%20Regression.ipynb) )
  
* Data Visualization - Dashboard created in Looker Studio (Dashboard available [here](https://lookerstudio.google.com/reporting/f2c52047-cbc5-4434-a196-1806aac77dbd) )
  
* Executive Report - Google Sheets document detailing the full analysis (available [here](https://docs.google.com/document/d/1xpuvG_GD15BuiaSIiabOqaPDJRFKKSung6jE8aLHSyA/edit?usp=sharing) )

### Analysis techniques
* Linear regression modelling (Python)
* Adapted RFM analysis
* Cohort analysis

### Insights and Recommendations
The project revealed several interesting and actionable insights, including;
* When producing academic publications, emphasis should be placed on targeting journals with a high Journal Impact Score (essentially, journals with proven record of producing impactful articles).
* Datasets segmented into the Gold Star segment, should be incorporated into the company database first as they have proven to be most relevant, impactful and accessible.
* No legal patents, related to ML, were found within the European jurisdication, however further analysis should be undertaken with a more extensive and robust dataset.

## Personal Reflections
This project represents the culmination of my studies within Turing College and was an extremely valuable exercise for learning. I gained a much better appreciation for the difficulties in working with raw, uncleaned and 'difficult' data (compared with the simplified and clean data in other course sprints). This project had multiple hurdles including learning to work with arrays within the data tables as well as understanding the limits of the interpretability of the data (eg. null values representing actual missing data or not) . Furthermore, I developed a much better understanding for the use-cases and applications of linear regression models, as in this case, the data was not entirely suitable for the desired outcome.

If I was to expand or repeat the project, I would search more extensively for suitable variables that could work as good predictors for the altmetric score of the publications. Secondly, in place of the adapted RFM analysis, I would developed a more refined classification technique (possibly using a decision-tree based model) to classify the datasets. There are some further improvements/adjustments I would also make to the final dashboard to make some of the charts clearer and more intuitive.
