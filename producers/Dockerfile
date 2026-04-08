FROM python:3.11-slim
WORKDIR /app

COPY producers/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY producers/producer_energy_ticks.py /app/producer_energy_ticks.py
CMD ["python", "/app/producer_energy_ticks.py"]
