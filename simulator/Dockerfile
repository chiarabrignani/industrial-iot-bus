FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY bus_simulator.py .

CMD ["python", "bus_simulator.py"]