FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .

ENV PORT=8080
CMD ["functions-framework", "--target=load_hr_snapshot", "--port=8080"]
