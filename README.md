# Customer Feedback Early Warning System

### Real-Time Customer Feedback Analysis Using PySpark, NLP, Cohere, Gradio, PostgreSQL and Grafana

An end-to-end, near-real-time customer feedback analysis system that processes historical and incoming product reviews, identifies emerging customer complaints, generates AI-assisted issue reports and visualizes customer sentiment through an interactive monitoring dashboard.

The project demonstrates how traditional batch analysis can be extended into a proactive feedback intelligence system using data engineering, natural language processing, generative AI and business intelligence.

> **Project type:** Academic case study and working prototype
> **Primary product in the demonstration:** `PHONE-001`
> **Development environment:** Google Colab
> **Data:** Synthetic historical reviews and simulated live customer reviews

---

## 1. Project Overview

Businesses receive customer feedback through product reviews, customer support channels and online platforms. Reviewing this information manually becomes difficult as the number of reviews increases. Traditional sentiment analysis can classify feedback as positive, negative or neutral, but a sentiment label alone does not tell a product manager whether a new problem is emerging or what action should be taken.

This project extends sentiment analysis into a proactive early-warning system.

The system processes a historical dataset and continuously ingests new reviews using PySpark Structured Streaming. Each review undergoes text cleaning, sentiment analysis, named entity recognition and product-theme extraction.

A monitoring process compares recent negative feedback against historical activity. When a potential issue is detected, the Cohere API generates a natural-language report containing an issue summary, supporting evidence and possible follow-up actions.

Gradio provides an interactive interface for product managers to inspect issues, request reports and inject new reviews. Processed data and alerts are synchronized to PostgreSQL, which Grafana uses for real-time-style KPI visualization.

### Key capabilities

* Historical review processing with PySpark.
* Continuous ingestion of incoming JSON reviews using Spark Structured Streaming.
* Text cleaning and normalization.
* VADER-based sentiment analysis.
* General named entity recognition using spaCy.
* Keyword-based product-feature and theme extraction.
* Historical-versus-recent negative sentiment comparison.
* Automated issue detection and Cohere-generated reports.
* Gradio dashboard with interactive issue investigation and review submission.
* PostgreSQL integration for Grafana visualization.
* Unified historical and incoming dataset exported in Parquet format.

---

## 2. Problem Statement

The objective is to transform an existing customer feedback analysis project into a proactive system capable of:

1. Processing new customer reviews as they arrive.
2. Identifying the product features or service areas discussed in each review.
3. Tracking sentiment trends over time.
4. Detecting sudden increases in negative feedback.
5. Automatically generating actionable issue reports.
6. Providing interactive investigation tools for product managers.
7. Displaying review and alert metrics through Grafana.

The implementation uses simulated review streams to demonstrate the complete workflow.

---

## 3. System Architecture

```text
          HISTORICAL CUSTOMER REVIEWS
                    CSV
                     |
                     v
              PySpark Batch ETL
                     |
                     |
NEW CUSTOMER REVIEWS |
       JSON          |
        |            |
        v            |
 PySpark Structured  |
     Streaming       |
        |            |
        +------------+
                     |
                     v
              NLP Processing
          -----------------------
          Text Cleaning
          VADER Sentiment
          spaCy General NER
          Product-Theme Matching
          -----------------------
                     |
                     v
                SQLite
          Processed Reviews
             Issue Alerts
                     |
          +----------+----------+
          |                     |
          v                     v
   Issue Detection        PostgreSQL Sync
          |                     |
          v                     v
      Cohere API              Grafana
          |               KPI Visualization
          v
   AI-Generated Reports
          |
          v
        Gradio
   Early Warning Dashboard
   Interactive Investigation
   Live Review Submission
```

A separate PySpark operation combines the historical and incoming datasets into a deduplicated, unified Parquet snapshot for subsequent analytical processing.

### Processing model

The prototype uses micro-batch streaming rather than processing every review as an independent event. Spark checks the incoming directory using a configured ten-second processing trigger.

The issue-monitoring process checks for emerging problems approximately every 60 seconds, while the PostgreSQL synchronization thread runs approximately every 30 seconds.

Actual end-to-end latency depends on data volume, Spark processing time, database synchronization and external API response times.

---

## 4. Technology Stack

| Technology                 | Purpose                                              |
| -------------------------- | ---------------------------------------------------- |
| Python                     | Main programming language                            |
| Google Colab               | Notebook-based development and execution             |
| Google Drive               | Persistent storage for project files and checkpoints |
| PySpark                    | Historical ETL and streaming processing              |
| Spark Structured Streaming | Micro-batch ingestion of new reviews                 |
| pandas and NumPy           | Data manipulation and analytical calculations        |
| VADER                      | Rule-based sentiment classification                  |
| spaCy                      | General named entity recognition                     |
| Cohere API                 | AI-assisted issue summaries and recommendations      |
| Gradio                     | Interactive early-warning application                |
| Plotly                     | Visualizations inside Gradio                         |
| SQLite                     | Prototype processed-data and alert storage           |
| PostgreSQL                 | Database used by Grafana                             |
| psycopg2                   | PostgreSQL connectivity and synchronization          |
| Grafana                    | KPI monitoring and sentiment visualization           |
| Parquet                    | Column-oriented unified dataset storage              |

**Supabase:** The system can use Supabase as a managed PostgreSQL provider. If the configured PostgreSQL connection string points to Supabase, it hosts the database used by Grafana. The notebook itself uses standard PostgreSQL connectivity through `psycopg2`.

---

## 5. Repository Structure

```text
customer-feedback-early-warning-system/
|
|-- notebooks/
|   |-- customer_feedback_analysis.ipynb
|
|-- docs/
|   |-- presentation.pptx
|   |-- technical_report.pdf
|   |-- architecture.png
|
|-- screenshots/
|   |-- gradio_dashboard.png
|   |-- grafana_dashboard.png
|
|-- requirements.txt
|-- .env.example
|-- .gitignore
|-- README.md
|-- LICENSE
```

The `architecture.png` and screenshot files are optional documentation assets. Add them after capturing or preparing the corresponding images.

Generated databases, incoming review files, Spark checkpoints and credentials should not be committed to the public repository.

---

## 6. Dataset Description

### Historical dataset

The notebook generates a synthetic historical dataset containing 1,200 customer reviews for the demonstration product `PHONE-001`.

Reviews are selected from predefined examples and assigned timestamps from approximately one to fourteen days before dataset generation.

The historical dataset establishes a baseline for comparing recent customer sentiment with previous feedback.

### Incoming dataset

Incoming reviews are stored as JSON files in a directory monitored by Spark Structured Streaming.

Each review contains fields similar to:

```json
{
  "review_id": "review-unique-id",
  "product_id": "PHONE-001",
  "review_text": "The battery drains quickly after the update.",
  "event_time": "2026-09-29T10:30:00+00:00",
  "source": "live"
}
```

The system assigns unique identifiers to new reviews to support deduplication.

### Demonstration dataset

A separate test function generates 110 synthetic incoming reviews covering:

| Theme            | Reviews |
| ---------------- | ------: |
| Battery life     |      52 |
| Delivery         |      23 |
| Pricing          |      14 |
| Customer service |      12 |
| Performance      |       9 |
| **Total**        | **110** |

These reviews contain predefined examples intended to represent positive, negative and neutral feedback.

The intended categories in the test plan are not ground-truth sentiment labels. The actual sentiment assigned to each review is determined independently by VADER.

---

## 7. Data Ingestion and ETL

### Historical data ingestion

The historical dataset is read from CSV using PySpark's batch DataFrame API.

An explicit schema defines the expected columns and data types. This ensures consistent processing across the historical and incoming datasets.

### Streaming ingestion

Spark Structured Streaming monitors the incoming directory for new JSON review files.

The streaming reader uses an explicit schema and limits the number of new files processed in each trigger.

The notebook configures a processing-time trigger of ten seconds.

### Micro-batch processing

The `foreachBatch()` API allows every incoming micro-batch to pass through the same transformation logic used for historical reviews.

This reduces inconsistencies between batch and streaming processing.

### Checkpointing

Spark stores streaming progress in a checkpoint directory on Google Drive.

The checkpoint supports recovery when the streaming application restarts, provided the checkpoint and associated source data are preserved.

The project should not delete an active checkpoint while the streaming query is running.

---

## 8. Natural Language Processing

### Text cleaning

The text-processing function removes URLs, normalizes excessive whitespace and creates a lowercase representation of each review.

This helps make keyword matching more consistent.

### Sentiment analysis using VADER

VADER produces a compound sentiment score for each review.

The notebook classifies reviews using these thresholds:

| Compound score                | Sentiment |
| ----------------------------- | --------- |
| Greater than or equal to 0.05 | Positive  |
| Less than or equal to -0.05   | Negative  |
| Between -0.05 and 0.05        | Neutral   |

VADER is computationally lightweight and appropriate for a prototype.

However, it can misclassify sarcasm, domain-specific expressions and reviews containing conflicting opinions.

### Named entity recognition using spaCy

The notebook uses spaCy's `en_core_web_sm` model for general named entity recognition.

Depending on the text, spaCy can identify entities such as organizations, locations, dates and other supported entity types.

### Product-feature extraction

Product features and themes are identified using a predefined keyword dictionary.

The supported themes include:

* Battery life
* Charging
* Delivery
* Pricing
* Customer service
* Display
* Performance

For example, reviews containing expressions such as "battery drains" or "battery backup" can be associated with the battery-life theme.

The notebook can identify multiple matched features but selects a primary product theme for grouping and reporting.

**Important distinction:** Product-feature extraction is keyword-based. The general spaCy model is not a custom-trained product-feature NER model.

---

## 9. Structured Processed Dataset

After ETL, each processed review contains the information needed for analysis and visualization.

The denormalized dataset includes review identification, product identification, original and cleaned text, timestamps, source, sentiment label, sentiment score, product theme, extracted product features and recognized entities.

A denormalized design allows Grafana and the issue-detection functions to query processed records without repeatedly reconstructing the NLP results.

SQLite stores the processed records in the `processed_reviews` table.

The `issue_alerts` table stores generated warnings and their associated reports.

---

## 10. Historical and Live Data Integration

The `build_unified_snapshot()` function combines the historical dataset with incoming review data.

Its workflow is:

1. Read and transform historical reviews.
2. Read and transform incoming reviews.
3. Use a left anti join to exclude incoming review IDs already present in the historical dataset.
4. Combine the datasets using `unionByName()`.
5. Remove duplicate review IDs.
6. Write the resulting dataset in Parquet format.

The left anti join prevents reviews already included in the historical dataset from being added a second time.

Parquet is used because it provides efficient column-oriented storage for subsequent analytical workloads.

The unified Parquet dataset is a batch-generated snapshot. It is separate from the PostgreSQL tables that Grafana queries.

---

## 11. Emerging Issue Detection

The early-warning mechanism compares recent negative feedback against historical activity.

### Current observation window

The system counts negative reviews from the latest 24 hours for each product and theme.

### Historical baseline

It calculates the negative-review count for the preceding seven days and divides the total by seven to obtain a daily average.

### Surge ratio

```text
Baseline daily average =
    Historical negative reviews / 7

Surge ratio =
    Negative reviews in the last 24 hours
    / Baseline daily average
```

### Detection conditions

A historical surge candidate requires:

* At least five negative reviews in the current 24-hour window.
* A surge ratio of at least two relative to the historical daily average.

If the historical baseline is zero, five or more recent negative reviews can be classified as a new issue or an issue with insufficient baseline data.

These thresholds are prototype design choices rather than statistically validated anomaly-detection parameters.

### Alert deduplication

Before generating an alert, the system checks whether an alert for the same product and theme has already been created within the previous 24 hours.

This reduces repeated reports for the same emerging issue.

---

## 12. Cohere-Based Issue Reports

The Cohere API converts structured issue metrics and supporting review excerpts into natural-language reports.

The report-generation function receives information such as the product ID, product theme, negative-review count, historical baseline, surge ratio and recent negative review excerpts.

The prompt asks Cohere to generate an issue summary, describe the supporting evidence, distinguish possible causes from confirmed facts and suggest follow-up actions.

The notebook limits the supporting evidence to a selection of recent reviews and uses a relatively low generation temperature to encourage consistent output.

Generated reports are stored with their alerts and can be accessed through Gradio.

Cohere provides AI-assisted analysis; its suggested causes and actions should be reviewed by the product team before operational decisions are made.

---

## 13. Gradio Early Warning System

Gradio provides an interactive web application for the prototype.

### Early Warning Dashboard

The dashboard summarizes recent review activity and displays the most frequent negative product themes.

It also presents recent issue alerts and provides controls for refreshing the displayed information.

### Investigate an Issue

A product manager can select a product theme and request a detailed report.

The application retrieves relevant recent negative reviews and sends them to Cohere for an evidence-based summary and suggested follow-up actions.

### Inject Live Review

The application provides a form for entering a product ID and review text.

Submitting the form creates a uniquely identified JSON review file in the incoming directory.

Spark subsequently processes the review during a streaming micro-batch.

The user can refresh the dashboard to inspect the updated results.

Gradio is primarily responsible for user interaction and issue investigation, while Grafana is responsible for operational monitoring.

---

## 14. Database Integration

### SQLite

SQLite is used as the prototype's processed-data and alert store.

The notebook creates two main tables:

* `processed_reviews`
* `issue_alerts`

### PostgreSQL

PostgreSQL stores copies of the processed reviews and generated alerts for Grafana.

The `sync_postgres()` function reads records from SQLite and inserts them into PostgreSQL using `psycopg2`.

Batch insertion uses `execute_values()`.

The SQL uses `ON CONFLICT DO NOTHING` to avoid inserting duplicate primary keys.

A background synchronization thread runs approximately every 30 seconds.

**Limitation:** The current synchronization strategy is insert-only. Subsequent updates and deletions in SQLite are not automatically propagated to PostgreSQL. For production use, an upsert strategy and explicit deletion handling would be needed.

---

## 15. Grafana Monitoring Dashboard

Grafana connects to the PostgreSQL database and visualizes the processed review and alert tables.

The proposed monitoring dashboard contains six panels.

### Panel 1: Daily Review Volume

Displays the number of customer reviews received during the selected period.

```sql
SELECT COUNT(*) AS review_count
FROM processed_reviews
WHERE $__timeFilter(event_time);
```

### Panel 2: Proactive Issue Alerts

Displays the number of alerts generated in the last 24 hours.

```sql
SELECT COUNT(*) AS alert_count
FROM issue_alerts
WHERE created_at >= NOW() - INTERVAL '24 hours';
```

### Panel 3: Negative Sentiment Trend

An hourly time-series chart showing the number of negative reviews.

```sql
SELECT
    $__timeGroupAlias(event_time, '1h'),
    COUNT(*) AS negative_reviews
FROM processed_reviews
WHERE sentiment = 'negative'
  AND $__timeFilter(event_time)
GROUP BY 1
ORDER BY 1;
```

### Panel 4: Top Five Product Themes

A pie or donut chart displaying the most frequently discussed product themes.

```sql
SELECT
    product_theme,
    COUNT(*) AS review_count
FROM processed_reviews
WHERE $__timeFilter(event_time)
GROUP BY product_theme
ORDER BY review_count DESC
LIMIT 5;
```

To show only negative themes, add:

```sql
AND sentiment = 'negative'
```

to the `WHERE` clause.

### Panel 5: Sentiment Distribution

Displays the relative volume of positive, negative and neutral feedback.

```sql
SELECT
    sentiment,
    COUNT(*) AS review_count
FROM processed_reviews
WHERE $__timeFilter(event_time)
GROUP BY sentiment;
```

### Panel 6: Latest Issue Alerts

Displays recent warnings with their product, theme, severity and review counts.

```sql
SELECT
    created_at,
    product_id,
    product_theme,
    severity,
    negative_count,
    baseline_count
FROM issue_alerts
WHERE $__timeFilter(created_at)
ORDER BY created_at DESC
LIMIT 20;
```

For the live demonstration, the dashboard can use a last-24-hours time range and an automatic refresh interval of approximately 30 seconds.

---

## 16. Installation and Configuration

### Prerequisites

* Google account and Google Colab
* Google Drive storage
* Cohere API key
* Accessible PostgreSQL database
* Grafana instance with PostgreSQL configured as a data source
* Compatible Java installation for the selected PySpark version

The project was developed in Colab. Local execution requires adjustments to Google Drive paths, Colab Secrets and environment-specific configuration.

### Step 1: Open the notebook

Open `notebooks/customer_feedback_analysis.ipynb` in Google Colab.

### Step 2: Configure secrets

In Colab, open the Secrets panel and add:

```text
COHERE_API_KEY
POSTGRES_DSN
```

The PostgreSQL connection string should contain the credentials and connection parameters for your own database.

Do not publish actual secret values.

### Step 3: Install dependencies

Run the notebook's installation cell.

Use the working PySpark and Java configuration appropriate for your Colab runtime. Restart the runtime after dependency installation if required.

### Step 4: Mount Google Drive

Run the Drive mounting cell and authorize access.

The notebook creates its project directories under:

```text
/content/drive/MyDrive/customer_feedback_realtime
```

### Step 5: Initialize the system

Run the configuration, Spark, NLP, database and historical-processing cells.

Start the Spark Structured Streaming query, issue-monitoring thread and PostgreSQL synchronization thread.

### Step 6: Launch Gradio

Run the Gradio launch cell.

A temporary public Gradio URL will be displayed when sharing is enabled.

### Step 7: Open Grafana

Configure Grafana's PostgreSQL data source and import or create the monitoring panels.

Set the dashboard time range and refresh interval.

---

## 17. Demonstration and Testing

The notebook includes a bulk-testing function named:

```python
run_full_dashboard_test()
```

It generates 110 synthetic reviews across five themes and writes them into the incoming directory.

The function waits for Spark to process the reviews, runs emerging-issue detection, generates eligible alerts, synchronizes PostgreSQL and creates a unified dataset snapshot.

Run the function explicitly only when you want to conduct a bulk test.

```python
unified = run_full_dashboard_test()
```

### Observed notebook results

In the saved demonstration run, the notebook recorded:

* 1,200 historical reviews.
* 110 injected test reviews processed by the streaming pipeline.
* Two alerts returned by the explicit test call.
* A unified dataset containing 1,313 records after test activity.

The detected counts can differ from the intended test-plan sentiment distribution because VADER classifies each review independently.

Alert counts can also vary depending on the historical baseline, previously generated alerts, detection thresholds and background-monitor timing.

These figures describe a saved demonstration run, not a guaranteed outcome of every execution.

---

## 18. Important Execution Notes

The installation, initialization and testing stages should be kept separate.

A normal application startup should not automatically clear historical data, delete databases or inject the 110-review test batch.

The destructive reset operation should be enabled only for a deliberately fresh demonstration.

The bulk test should be executed manually after Spark streaming and database synchronization are running.

The unified snapshot should be generated when a current historical-plus-incoming dataset is required.

If test reviews are removed from the databases, previously generated alerts, incoming JSON files and Parquet snapshots may require separate cleanup.

Do not delete Spark checkpoints while the streaming query is active.

---

## 19. Security and Privacy

The notebook retrieves its Cohere API key and PostgreSQL connection string from Colab Secrets.

Before publishing the notebook:

* Remove all hardcoded credentials, if any.
* Clear saved notebook outputs.
* Do not publish database files or connection strings.
* Exclude generated checkpoints and temporary files.
* Use synthetic or appropriately anonymized review data for public demonstrations.
* Review generated Cohere reports before sharing them publicly.

A `.gitignore` file should exclude `.env` files, local databases, checkpoints, generated outputs and temporary review files.

---

## 20. Current Limitations

This project is a working academic prototype rather than a production-ready feedback platform.

The principal limitations are:

**Synthetic baseline:** Historical reviews are generated from predefined examples rather than collected from real customer activity.

**Rule-based sentiment:** VADER is lightweight but may misclassify complex or domain-specific language.

**Keyword-based themes:** Product themes are identified through predefined keywords rather than a trained multi-label classifier.

**General-purpose NER:** spaCy extracts general entities; product-feature recognition is not based on a custom NER model.

**Micro-batch latency:** The ten-second Spark trigger is not an end-to-end processing guarantee.

**SQLite scalability:** Converting Spark batches to pandas and inserting into SQLite is suitable for a small prototype but not large-scale production traffic.

**Insert-only synchronization:** PostgreSQL synchronization does not automatically propagate SQLite updates or deletions.

**Threshold-based detection:** Fixed thresholds may not perform equally well for products with very different review volumes.

**Generative AI reliability:** Cohere reports can contain unsupported interpretations and require human review.

**Notebook lifecycle:** Background threads and streaming processes stop when the Colab runtime terminates.

---

## 21. Future Enhancements

Potential improvements include integrating real customer-feedback APIs, replacing directory-based ingestion with a message broker, training a domain-specific sentiment model, adding custom product-feature NER, supporting multi-label theme classification and introducing statistical anomaly detection.

A production deployment could use scalable streaming storage, direct PostgreSQL or data-warehouse ingestion, scheduled model evaluation, alert notification channels and containerized deployment.

Additional evaluation could measure sentiment accuracy, theme-classification precision and recall, alert precision, processing latency and the quality of generated reports.

---

## 22. Academic Learning Outcomes

This case study demonstrates the integration of multiple technologies into a complete data-processing workflow.

The project covers batch and streaming ETL, natural language processing, historical trend analysis, threshold-based anomaly detection, generative AI integration, interactive application development, database synchronization and operational visualization.

Its central contribution is moving beyond individual sentiment labels toward a system that identifies patterns of emerging negative feedback and presents them in a form that product managers can investigate.

---

## 23. References

* [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
* [Spark Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/streaming/index.html)
* [spaCy Documentation](https://spacy.io/usage)
* [VADER Sentiment Analysis](https://github.com/cjhutto/vaderSentiment)
* [Cohere Documentation](https://docs.cohere.com/)
* [Gradio Documentation](https://www.gradio.app/docs)
* [PostgreSQL Documentation](https://www.postgresql.org/docs/)
* [Grafana Documentation](https://grafana.com/docs/)

---

**Disclaimer:** This repository demonstrates a synthetic-data academic prototype. Generated issue reports are decision-support material and should not be treated as independently verified diagnoses of real product defects.
