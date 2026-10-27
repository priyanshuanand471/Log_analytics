FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir dash==2.18.2 plotly==5.24.1 cassandra-driver==3.29.2 pandas==2.2.3
COPY dashboard/app.py /app/app.py
EXPOSE 8050
CMD ["python", "app.py"]
