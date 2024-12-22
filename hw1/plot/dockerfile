FROM python:3.9
WORKDIR /app
COPY ./src ./
COPY ./requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
RUN mkdir -p /app/logs
ENV MPLBACKEND=Agg
CMD ["python", "plot.py"]
