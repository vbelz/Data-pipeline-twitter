import pandas as pd
import slack
import datetime
import asyncio
import nest_asyncio
import time
from sqlalchemy import create_engine

# connection infos
HOST_POSTGRES = 'postgresdb'   #Host name is the container name in docker-compose file
PORT_POSTGRES = '5432'
DBNAME_POSTGRES = 'twitter_database'
USERNAME='admin'
PASSWORD='admin'
table='tweets_data'

while True:

    time.sleep(3600)

    connection_string = 'postgres://admin:admin@postgresdb:5432/twitter_database'
    engine_postgres = create_engine(connection_string)

    #Query to select the last hour data
    query = f'''SELECT * FROM {table}
    WHERE time > (SELECT MAX(time)-INTERVAL '1 hour' FROM {table})
    ;'''

    df_recent = pd.read_sql(query, engine_postgres)

    print("Recent data loaded")
    print(df_recent.shape)

    market_sentiment = round(df_recent['sentiment'].value_counts()* 100/df_recent['sentiment'].count(),2)

    #print(f"{datetime.datetime.now()} : The crypto Market is currently {market_sentiment['neutral']}% neutral, {market_sentiment['positive']}% bullish and {market_sentiment['negative']}% bearish")
    #print('\nStrategy recommended: HODL\n')

    TEXT = f'''{datetime.datetime.now()} : The crypto Market is currently {market_sentiment['neutral']}% neutral,
    {market_sentiment['positive']}% bullish and {market_sentiment['negative']}% bearish.
    '''

    print(Text)


    nest_asyncio.apply()

    #Need to register an app at slack.api and get 'Bot User OAuth Access Token'
    token = '...'
    client = slack.WebClient(token=token)

    response = client.chat_postMessage(channel='#slackbot_testing', text=TEXT)
