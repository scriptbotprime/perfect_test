FROM prefecthq/prefect:3.8.3-python3.14

WORKDIR /flows
COPY flows/ .

CMD ["python", "runner.py"]
