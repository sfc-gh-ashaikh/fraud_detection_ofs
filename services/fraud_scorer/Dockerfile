FROM python:3.11-slim

WORKDIR /app

# Install dependencies. No Snowflake connector — model loads from mounted stage volume.
# SNOWFLAKE_TOKEN is auto-injected by SPCS; no credentials in the image.
RUN pip install --no-cache-dir \
    fastapi==0.115.0 \
    uvicorn[standard]==0.32.0 \
    xgboost==2.1.1 \
    requests==2.32.3 \
    numpy==1.26.4

COPY app.py .

EXPOSE 8080

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "2"]
