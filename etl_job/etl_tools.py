from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import pandas as pd
import pymongo
import re

analyser = SentimentIntensityAnalyzer()

def length_text(sentence):
    "Return number of character of a text"
    return len(sentence)

def clean_text(sentence):
    "Clean the text of the tweets"
    text = re.sub('[)(#$]', ' ', sentence)
    text = re.sub("\n", " ", text)
    text = re.sub('(www|http:|https:)+[^\s]+[\w]',' ', text)
    return text

def negativity_score(sentence):
    "Return the negativity score of the tweet"
    score = analyser.polarity_scores(sentence)
    return score['neg']

def positivity_score(sentence):
    "Return the positivity score of the tweet"
    score = analyser.polarity_scores(sentence)
    return score['pos']

def neutral_score(sentence):
    "Return the neutral score of the tweet"
    score = analyser.polarity_scores(sentence)
    return score['neu']

def compound_score(sentence):
    "Return the compound score of the tweet"
    score = analyser.polarity_scores(sentence)
    return score['compound']

def label_sentiment(global_nb):
    "Label the tweet from compound_score"
    if global_nb > 0.05:
        label = 'positive'
    elif global_nb < -0.05:
        label = 'negative'
    else:
        label = 'neutral'

    return label
