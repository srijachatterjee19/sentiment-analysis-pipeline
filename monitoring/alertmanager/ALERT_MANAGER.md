# 📧 Email Alert Setup

## ✅ Alertmanager Email Configuration

- Uses Gmail SMTP 
- Triggers on Airflow DAG failures

## 🔧 Setup Instructions

1. Replace the values in `alertmanager.yml`:
   - `your-email@gmail.com`
   - `your-app-password` (use App Password from Google account security settings)
   - `recipient@example.com`

2. Restart Alertmanager:

```bash
docker-compose -f docker-compose.monitoring.yml restart alertmanager
```

3. Verify at: http://localhost:9093

You will receive email alerts if DAG `sentiment_pipeline_spark` fails.