# -*- coding: UTF-8 -*-

import os
import numpy as np
from scipy import spatial
from operator import itemgetter
from pyspark.mllib.feature import HashingTF, IDF
from sklearn.feature_extraction.text import TfidfTransformer
from pyspark import SparkContext, SparkConf
from sklearn.feature_extraction.text import CountVectorizer
import logging, jieba
# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from util import preprocess_per_news
from pyspark import SparkContext
from pyspark.sql import SQLContext,Row


class RecommendationEngine:
    """A movie recommendation engine
    """

   # get the news that user read and return the parsed news list
    def getUserReadNews(self, user_id):

        sqlContext = SQLContext(self.sc)

        # load records
        df_r = sqlContext.read.format('jdbc').options(
            url="jdbc:mysql://localhost/RS_News?user=root&password=10081008", dbtable="app_userrecords").load()
        records = df_r.filter(df_r["user_id"] == user_id)
        records_list = [i.news_id for i in records.collect()]

        # # load read news
        df_news = sqlContext.read.format('jdbc').options(
            url="jdbc:mysql://localhost/RS_News?user=root&password=10081008", dbtable="app_caixinnews").load()

        user_news_df = df_news.filter(df_news['news_id'].isin(records_list))
        user_news = [preprocess_per_news(i.content) for i in user_news_df.collect()]

        user_topics = [i.topic for i in user_news_df.collect()]
        candidates_df = df_news.filter(df_news['topic'].isin(user_topics))
        candidates = [preprocess_per_news(i.content) for i in candidates_df.collect()]
        candidates_newsid = [i.news_id for i in candidates_df.collect()]
        return user_news, candidates, candidates_newsid

    def getRecommendation(self, user_id):

        user_news, candidates_news, candidates_newsid = self.getUserReadNews(user_id)
        all_news = user_news + candidates_news

        # 将文本中的词语转换为词频矩阵
        vectorizer = CountVectorizer()
        # 计算个词语出现的次数
        X = vectorizer.fit_transform(all_news)
        # 获取词袋中所有文本关键词
        # word = vectorizer.get_feature_names()

        transformer = TfidfTransformer()
        # 将词频矩阵X统计成TF-IDF值
        tfidf = transformer.fit_transform(X).toarray()
        # 查看数据结构 tfidf[i][j]表示i类文本中的tf-idf权重
        # print tfidf.toarray()

        recommend_num = 10
        recommend_per_news = recommend_num / len(user_news)
        recommend_list = []
        user_news_len = len(user_news)
        candidates_news_len = len(candidates_news)

        for i in range(user_news_len):
            news_candidate_sim = []
            for j in range(candidates_news_len):
                sim = 1 - spatial.distance.cosine(tfidf[i], tfidf[j + user_news_len])
                news_candidate_sim.append(sim)
            k_max_index = (-np.array(news_candidate_sim)).argsort()[:recommend_per_news]
            recommend_list.extend(k_max_index)

        recommend_news_id = [candidates_newsid[i] for i in recommend_list]
        return recommend_news_id

    # def getKeywords(self):
    #
    #     news = sc.parallelize(self.getUserReadNews())
    #     x = news.collect()
    #     hashing = HashingTF()
    #
    #     news_tf = hashing.transform(news)
    #     idfIgnore = IDF(minDocFreq=2).fit(news_tf)
    #     result = idfIgnore.transform(news_tf)

    def extractKeywords_Train(self):
        documents = self.sc.textFile(self.trainingfile).map(lambda line: line.split(" ")[1:])

        hashingTF = HashingTF()
        tf = hashingTF.transform(documents)
        tf.cache()

        idfIgnore = IDF(minDocFreq=2).fit(tf)
        tfidfIgnore = idfIgnore.transform(tf)

        tfidfIgnore.saveAsTextFile("AAA")


    def __init__(self, sc):
        """Init the recommendation engine given a Spark context and a dataset path
        """

        logger.info("Starting up the Recommendation Engine: ")

        self.sc = sc

        # Load ratings data for later use
        logger.info("Engine init lalalal...")
        # self.trainingfile = os.path.join(dataset_path, 'training_data.txt')
