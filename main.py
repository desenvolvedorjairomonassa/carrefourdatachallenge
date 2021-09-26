import os
from env import ACCESS_TOKEN, ACCESS_TOKEN_SECRET, API_KEY, API_KEY_SECRET, MONGO_PASSWORD, MONGO_USER, IBM_APIKEY, IBM_URL_NLU
import tweepy
from pymongo import MongoClient
import json
import pandas as pd
from ibm_watson import NaturalLanguageUnderstandingV1
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from ibm_watson.natural_language_understanding_v1 import Features, SentimentOptions

auth = tweepy.OAuthHandler(API_KEY, API_KEY_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
def configurar_acesso_api_tweet():
    return tweepy.API(auth)

def trends_api_tweet(api):
    try:
        BRASIL_WOEID = 23424768        
        trendstopics= api.trends_place(BRASIL_WOEID)
        return [trend for trend in trendstopics]
        #for tweet in trendstopics:
        #    print(tweet)
    except Exception as error:    
        print('Erro na API: %s'%(error))

def trazer_um_trendingtopic():
    try:    
        client = MongoClient("mongodb+srv://%s:%s@monassa.vgddz.mongodb.net/tweet?retryWrites=true&w=majority" %(MONGO_USER,MONGO_PASSWORD))
        # print(client)
        db = client.get_database('tweet')
        return db.trendtopics.find_one()
        
    except Exception as inst:       
        print('erro no acesso ao banco : %s' %(inst))    

def gravar_mongo_trendingtopics(trends):
    #gravar na cloud mongo db
    try:    
        client = MongoClient("mongodb+srv://%s:%s@monassa.vgddz.mongodb.net/tweet?retryWrites=true&w=majority" %(MONGO_USER,MONGO_PASSWORD))
        # print(client)
        db = client.get_database('tweet')
        trend = db.trendtopics
        contagem = trend.count_documents({})
        print(contagem)
        if (trends):
            trend.insert_many(trends[0]["trends"])
    except Exception as inst:       
        print('erro no acesso ao banco : %s' %(inst))    

def gravar_mongo_historytweets(historico):
    #gravar na cloud mongo db
    try:    
        client = MongoClient("mongodb+srv://%s:%s@monassa.vgddz.mongodb.net/tweet?retryWrites=true&w=majority" %(MONGO_USER,MONGO_PASSWORD))
        # print(client)
        db = client.get_database('tweet')
        historicojson=[tweet._json for tweet in historico ]        
        if (historicojson):
            db.historytrend.insert_many(historicojson)
    except Exception as inst:       
        print('erro no acesso ao banco coleção histórico : %s' %(inst))    

def recuperar_trend_topics():
    try:
        client = MongoClient("mongodb+srv://%s:%s@monassa.vgddz.mongodb.net/tweet?retryWrites=true&w=majority" %(MONGO_USER,MONGO_PASSWORD))
        db = client.get_database('tweet')
        return list(db.trendtopics.find() )
    except Exception as inst:  
      print('erro no acesso ao banco coleção trendtopics : %s' %(inst))          

def trazer_historico_tweets():
    try:    
        client = MongoClient("mongodb+srv://%s:%s@monassa.vgddz.mongodb.net/tweet?retryWrites=true&w=majority" %(MONGO_USER,MONGO_PASSWORD))
        db = client.get_database('tweet')
        tr = db.historytrend.find()       
        return list(tr)
        
    except Exception as inst:       
        print('erro no acesso ao banco-coleção historico: %s' %(inst))

def converter_dataframe(hist_tweets):
    text=[]
    retweet=[]
    coordinates=[]
    created_at=[]
    location=[]
    favourites_count=[]
    followers_count=[]
    friends_count=[]
    statuses_count=[]
    q_retweet_count=[]
    score=[]
    sentiment=[]

    for tweets in hist_tweets:
        #pequeno tratamento do texto
        texto_mod = " ".join(filter(lambda x:x[0]!='@', tweets['text'].split()))        
        texto_mod = texto_mod.replace(',','')
        texto_mod = texto_mod.replace(';','')
        if (texto_mod.find('https')>0):
            texto_mod=texto_mod[:texto_mod.find('https')] 
        text.append(texto_mod)
        retweet.append(tweets['retweet_count'])
        coordinates.append(tweets['coordinates'])
        created_at.append(tweets['created_at'])
        location.append(tweets['user']['location'])
        favourites_count.append(tweets['user']['favourites_count'])
        followers_count.append(tweets['user']['followers_count'])
        friends_count.append(tweets['user']['friends_count'])
        statuses_count.append(tweets['user']['statuses_count']) 
        if 'quoted_status' in tweets.keys():
            q_retweet_count.append(tweets['quoted_status']['retweet_count'])
        else:
            q_retweet_count.append(0)
        
        #resultado da ibm cloud do servço de NLU - Natural Language Understanding
        if (len(texto_mod)>0):
            resultado=analisar_ibm_cloud_nlu_sentimento(texto_mod)
            score.append(resultado['score'])
            sentiment.append(resultado['label'])
        #print(resultado['score'],resultado['label'])

    data = {'text':text, 'retweet': retweet, 'coordinates': coordinates, 'created_at': created_at,'location':location,
    'favourites_count': favourites_count, 'followers_count': followers_count,'friends_count':friends_count, 'statuses_count':statuses_count ,
    'q_retweet_count':q_retweet_count, 'score':score, 'sentiment':sentiment }
    return pd.DataFrame(data)

def analisar_ibm_cloud_nlu_sentimento(textanalize):
    authenticator = IAMAuthenticator(IBM_APIKEY)
    natural_language_understanding = NaturalLanguageUnderstandingV1(
        version='2021-08-01',
        authenticator=authenticator
    )

    natural_language_understanding.set_service_url(IBM_URL_NLU)

    response = natural_language_understanding.analyze(
        text=textanalize,
        features=Features(sentiment=SentimentOptions())).get_result()
    #print(response['sentiment']['document']['score'],response['sentiment']['document']['label'])
    return response['sentiment']['document']
def trends_topics_converter_df(topics):
    name=[]
    volume=[]
    for trends in topics:
        name.append(trends['name'])
        if (trends['tweet_volume'] == None):
            volume.append(0)
        else:    
            volume.append(trends['tweet_volume'])
    data = {'name':name, 'volumne':volume }
    return pd.DataFrame(data)    

if __name__ == "__main__":
    api = configurar_acesso_api_tweet()
    #pegar trending topics
    trends=trends_api_tweet(api)
    
    #gravar no cloud mongodb
    gravar_mongo_trendingtopics(trends)

    #trazer um topicos para ver os tweets
    topico =trazer_um_trendingtopic()
    pesquisartopico=topico['name']
    
    #pesquisa um trending topics e ver os tweet sem retweets
    historico = api.search(q='\"{}" -filter:retweets'.format(pesquisartopico), result_type ='popular', lang='pt',count=102)
    
    #teste  historico = api.search(q='\"{}" -filter:retweets'.format('%22Alexandre+Garcia%22'), result_type ='mixed', lang='pt',count=100)  #37859
    
    #gravar os tweets na cloud mongodb
    gravar_mongo_historytweets(historico)
    
    #pegar os tweets do mongodb cloud
    hist_tweets = trazer_historico_tweets ()
    
    #gerar um csv dos dados tweet capturados e já processar o sentimento do texto 
    df=converter_dataframe(hist_tweets)
    df.to_csv('tweets_trend.csv',sep=';')

    df_trends = trends_topics_converter_df(recuperar_trend_topics())
    df_trends.to_csv('trendtopcis.csv',sep=';',index=None)    
    
