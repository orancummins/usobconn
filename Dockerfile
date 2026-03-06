FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

RUN mkdir -p /app/instance

EXPOSE 9093

CMD ["gunicorn", "--bind", "0.0.0.0:9093", "--workers", "1", "--threads", "8", "--timeout", "120", "wsgi:app"]
