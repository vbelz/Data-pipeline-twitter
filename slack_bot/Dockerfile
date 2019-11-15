FROM python:3.6-slim
WORKDIR /app3
ADD . /app3
RUN pip install --trusted-host pypi.python.org -r requirements.txt
CMD ["python", "slack_bot.py"]
