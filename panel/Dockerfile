FROM python:3.12-slim
RUN pip install --no-cache-dir paho-mqtt==2.1.0 flask==3.0.3 waitress==3.0.0
WORKDIR /app
COPY app.py .
COPY static ./static
CMD ["python", "-u", "app.py"]
