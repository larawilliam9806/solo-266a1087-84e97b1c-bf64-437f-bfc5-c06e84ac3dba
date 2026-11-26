FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Originals are streamed into this directory while a batch upload is in
# flight and stay there until a job's last execution copy finishes.  A named
# volume (see docker-compose.yml) backs it so the data survives image updates.
RUN mkdir -p /srv/staging
ENV THUMBNAIL_STAGING_DIR=/srv/staging

EXPOSE 8000

# Single process: the scheduler state is in-memory, so do not add --workers > 1.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
