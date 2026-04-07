FROM python:3.11-slim

WORKDIR /

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY daily_tree_observation_fix.py .

CMD ["python", "daily_tree_observation_fix.py"]