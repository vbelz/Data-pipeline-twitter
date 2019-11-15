FROM python:3.6-slim
WORKDIR /app
ADD . /app
RUN pip install --trusted-host pypi.python.org -r requirements.txt
CMD ["python","get_tweets_for_mongo.py"]
