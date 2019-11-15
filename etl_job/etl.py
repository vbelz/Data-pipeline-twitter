from sqlalchemy import create_engine
import pandas as pd
import pymongo
import requests
from etl_tools import clean_text, length_text, positivity_score
from etl_tools import negativity_score, neutral_score, compound_score, label_sentiment
import logging
from args import parser

# 5 levels: DEBUG < INFO < WARNING < ERROR < CRITICAL
logging.basicConfig(level=logging.INFO)

# connection infos
HOST_POSTGRES = 'postgresdb'
PORT_POSTGRES = '5432'
DBNAME_POSTGRES = 'twitter_database'
USERNAME='admin'
PASSWORD='admin'

HOST_MONGO = 'mongodb'
PORT_MONGO = '27021'
CLIENT = pymongo.MongoClient('mongodb')
DB = CLIENT.twitter_db

def extract_data(id_index):
    """Read data from Mongodb"""
    if id_index == 0:
        cursor = DB.tweets.find({})
        current_size = DB.tweets.find({}).count()
    else:
        cursor = DB.tweets.find({}).skip(id_index)
        current_size = DB.tweets.find({}).count()

    # Expand the cursor and construct the DataFrame
    df =  pd.DataFrame(list(cursor))
    logging.info("Mongo data loaded")
    logging.info(str(df.shape))   # logging
    print(df.columns.values)
    return df, current_size

def transform_data(df):
    """Update data for Postgres"""
    # Creation of time labels
    df['hour'] = pd.DatetimeIndex(df['time']).hour
    df['minute'] = pd.DatetimeIndex(df['time']).minute
    df['day'] = pd.DatetimeIndex(df['time']).day
    df['month'] = pd.DatetimeIndex(df['time']).month
    df['year'] = pd.DatetimeIndex(df['time']).year
    #Clean the text of the tweet
    df['clean text'] = df['text'].apply(clean_text)
    #Add a column length of the tweet
    df['length text'] = df['clean text'].apply(length_text)
    #Add a column positivity score of the tweet
    df['positivity'] = df['clean text'].apply(positivity_score)
    #Add a column negativity score of the tweet
    df['negativity'] = df['clean text'].apply(negativity_score)
    #Add a column neutral score of the tweet
    df['neutral'] = df['clean text'].apply(neutral_score)
    #Add a column compound score of the tweet
    df['compound'] = df['clean text'].apply(compound_score)
    #Add a column label sentiment of the tweet
    df['sentiment'] = df['compound'].apply(label_sentiment)
    #Select column to save to Postgres
    df = df[['time', 'followers','user_favorites_count', 'username', 'hour', 'minute', 'day',
           'month', 'year', 'clean text', 'length text', 'positivity',
           'negativity', 'neutral', 'compound', 'sentiment']]
    return df

def load_data(df, job_number):
    """Load data into Postgres"""
    connection_string = 'postgres://admin:admin@postgresdb:5432/twitter_database'
    engine = create_engine(connection_string)

    if job_number == 1:
        df.to_sql('tweets_data', engine, if_exists='replace')
    else:
        df.to_sql('tweets_data', engine, if_exists='append')
    logging.info(f"Postgres data loaded for job number {job_number}")
    logging.info(str(df.shape))

# Load the parameters from teh scheduler
args = parser.parse_args()

index_etl = args.index_mongo
job_number = args.job_number

#Extract data and saved the current index of Mongo data
df, id_index = extract_data(index_etl)
logging.info(f"id_index is now {id_index} for Mongo")
logging.info(f"job number is now {job_number} for ETL")
d={'index_saved':id_index,'job_nb':job_number}
save_index = pd.DataFrame(d, index=[0])

if job_number == 1 :
    save_index.to_csv('save_index.csv', index=None)
else:
    save_index.to_csv('save_index.csv', index=None, mode='a', header=False)

#Transform data for Postgres:
df = transform_data(df)
#Save data to Postgres
load_data(df, job_number)
