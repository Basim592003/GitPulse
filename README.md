# 🚀 GitPulse

**Predict GitHub repositories that will go viral before they blow up.**

GitPulse is an ML-powered platform that ingests 100K+ GitHub events per day, processes them through a medallion architecture, and predicts which repos will gain 20+ stars in the next 48 hours.

## Features

- **Real-time Ingestion**: Pulls data from GitHub Archive every day
- **Medallion Architecture**: Bronze → Silver → Gold data pipeline
- **ML Predictions**: MLP classifier with 85% recall on viral detection
- **Interactive Dashboard**: Streamlit UI showing top predicted repos
- **Automated Pipeline**: GitHub Actions for daily ingestion and monthly retraining

## How It Works

```
GitHub Archive (daily events)
        ↓
    Bronze Layer (raw JSON)
        ↓
    Silver Layer (filtered parquet)
        ↓
    Gold Layer (daily repo metrics)
        ↓
    Feature Engineering
        ↓
    ML Prediction
        ↓
    Dashboard
```

### Prediction Labels

| Label | Definition | Use Case |
|-------|------------|----------|
| **Viral** | 20+ stars in next 48h | Big repos gaining momentum |
| **Trending** | 3x baseline + 5 stars in 48h | Small repos growing fast |
| **Hot** | Both viral AND trending | Hidden gems |

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/yourusername/GitPulse.git
cd GitPulse
```

### 2. Install dependencies

```bash
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows
pip install -r requirements.txt
```

### 3. Configure environment

Create `.env` file:

```env
R2_ACCESS_KEY_ID=your_access_key
R2_SECRET_ACCESS_KEY=your_secret_key
R2_ENDPOINT_URL=https://your-account.r2.cloudflarestorage.com
R2_BUCKET=gitpulse-data
```

### 4. Initial backfill (one-time)

```bash
python ingest/backfill.py
```

This downloads 8 days of historical data needed for predictions.

### 5. Run predictions

```bash
python ml/predict.py
```

### 6. Launch dashboard

```bash
streamlit run app/dashboard.py
```

## Data Pipeline

### Bronze Layer
- **Source**: GitHub Archive (`https://data.gharchive.org`)
- **Format**: Compressed JSON (`.json.gz`)
- **Storage**: `bronze/year={year}/month={month}/day={day}/hour={hour}/events.json.gz`

### Silver Layer
- **Events kept**: WatchEvent, ForkEvent, PushEvent, PullRequestEvent, IssuesEvent
- **Columns**: event_type, repo_id, repo_name, actor_id, created_at
- **Format**: Parquet
- **Storage**: `silver/year={year}/month={month}/day={day}/events.parquet`

### Gold Layer
- **Aggregation**: Daily metrics per repo
- **Columns**: repo_id, repo_name, stars, forks, pushes, prs, issues, date
- **Format**: Parquet
- **Storage**: `gold/year={year}/month={month}/day={day}/metrics.parquet`

## Feature Engineering

| Feature | Formula | Description |
|---------|---------|-------------|
| `stars` | Count of WatchEvents | Today's stars |
| `forks` | Count of ForkEvents | Today's forks |
| `avg_stars_7d` | Mean of past 7 days | Baseline popularity |
| `star_velocity` | `stars / (avg_stars_7d + 1)` | Growth rate |
| `fork_ratio` | `forks / (stars + 1)` | Builder interest |
| `activity_score` | `pushes + prs + issues` | Development activity |


## Automation

### Daily Pipeline (GitHub Actions)

Runs at 6 AM UTC daily:
1. Ingests yesterday's data
2. Processes through bronze → silver → gold
3. Runs predictions
4. Updates `predictions/latest.parquet`

### Monthly Retrain

Runs on 1st of each month:
1. Loads last 30 days of gold data
2. Builds features and labels
3. Trains new model
4. Compares F1 with old model
5. Saves if better
6. Deletes old month's data

## Tech Stack

- **Data Processing**: Pandas, PyArrow
- **Storage**: Cloudflare R2 (S3-compatible)
- **ML**: Scikit-learn (MLP, RandomForest)
- **Dashboard**: Streamlit
- **Automation**: GitHub Actions
- **Data Source**: GitHub Archive

## Acknowledgments

- [GitHub Archive](https://www.gharchive.org/) for providing public GitHub event data
- [Cloudflare R2](https://www.cloudflare.com/products/r2/) for affordable object storage
