FROM python:3.9
WORKDIR /app
COPY ./src ./
COPY ./requirements.txt ./
RUN mkdir -p /app/logs
RUN pip install --no-cache-dir -r requirements.txt
CMD [ "python", "./metric.py" ]
