#!/usr/bin/env python
# coding: utf-8

# In[80]:


import  urllib
from urllib.request import urlopen
from bs4 import BeautifulSoup
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer 
import pandas as pd
from nltk.tokenize import RegexpTokenizer
import sys
import psycopg2
from tqdm import tqdm
tokenizer = RegexpTokenizer(r'\w+')


# In[3]:


def links_from_acc(soup):
    links = []
    for link in soup.findAll('a'):
        try:
            curr_l = link.get('href')
            if not (curr_l is None) and (len(curr_l) > 1) and ('/' in curr_l):
                curr = curr_l.split('/')
                if (len(curr) == 2) and curr_l[1].isupper():
                    links.append('https://twitter.com' + curr_l)
        except:
            print('exception' + 'in' + curr_l)
    return set(links)


# In[4]:


def get_tweets(url):
    tweets = []
    page = urlopen(url)
    soup = BeautifulSoup(page,'html.parser')
    count = 0                                    
    for count in range (20):
        try:
            tweets.append(soup.findAll('div', {'class': 'js-tweet-text-container'})[count].find('p').text)
        except:
            break
    return tweets


# In[63]:


def standardize_text(df, text_field):
    df[text_field] = df[text_field].str.replace(r"http\S+", "")
    df[text_field] = df[text_field].str.replace(r"http", "")
    df[text_field] = df[text_field].str.replace(r"@\S+", "")
    df[text_field] = df[text_field].str.replace(r"[^A-Za-z@]", " ")
    df[text_field] = df[text_field].str.replace(r"@", "at")
    df[text_field] = df[text_field].str.lower()
    return df


# In[92]:


def tokenize(text):
    stopWords = list(set(stopwords.words('english')))
    stopWords.extend('twitter')
    stopWords.extend('com')
    lemmatizer = WordNetLemmatizer()
    tokens = tokenizer.tokenize(text.lower())
#     tokens = nltk.word_tokenize(text.lower(), r'\w+')
    tokens = [lemmatizer.lemmatize(w) for w in tokens]
    for token in tokens:
        if token in stopWords:
            tokens.remove(token)
    return tokens


# In[38]:


def tostr(twl):
    return ' '.join(el for el in twl)


# In[50]:


link = 'https://twitter.com/elonmusk'
blogger = link.split('/')[-1]
page = urlopen(link)
soup = BeautifulSoup(page,'html.parser')
tweets = {}
tweets[blogger] = get_tweets(link)
urls = links_from_acc(soup)
for url in tqdm(urls):    
    tweets[url.split('/')[-1]] = get_tweets(url)


# In[93]:


df = pd.DataFrame(data={'blogger':list(tweets.keys()), 'tweets':list(tweets.values())})
df['tweets'] = df['tweets'].apply(tostr)
df = standardize_text(df, 'tweets')
df['tweet_token'] = df['tweets'].apply(tokenize)


# In[94]:


con = psycopg2.connect(
    database="twitter_analysis",
    user="postgres",
    password='7c056266',
    host='database-2.ceuwom91dpva.us-east-1.rds.amazonaws.com',
    port="5432",
)
con.autocommit = True
cur = con.cursor()


# In[88]:


for i in tqdm(range(len(df))):
    blogger = df['blogger'][i]
    for wr in df['tweet_token'][i]:
        query = list()
        query.append(blogger)
        query.append(wr)
        cur.execute("INSERT INTO tweets_words VALUES (%s, %s)", query)

