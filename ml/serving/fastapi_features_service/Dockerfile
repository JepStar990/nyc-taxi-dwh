FROM python:3.11-slim

WORKDIR /app
RUN pip install --no-cache-dir fastapi==0.115.0 uvicorn==0.30.6 \
    feast==0.43.1 redis==5.0.1 pydantic==2.8.2

COPY app.py /app/app.py
EXPOSE 8080
CMD ["uvicorn","app:app","--host","0.0.0.0","--port","8080"]
