import pandas as pd
import time
import numpy as np
from utils import *

def changeToTimestamp(sep_time):
    time_struct = time.strptime(sep_time, "%Y-%m-%d %H:%M:%S")
    timestamp = int(time.mktime(time_struct))
    return timestamp

def get_data(filename, sep, names):
    raw_data = pd.read_table(filename, sep='\t', header=None, names=names).dropna(how='any')

    read_times = raw_data['read_time']
    sep_time = "2014-03-20 23:59:00" # the time dividing train data and test data
    timestamp = changeToTimestamp(sep_time)

    before_sep_data = read_times.index[read_times < timestamp]
    after_sep_data = read_times.index[read_times >= timestamp]

    training_data = raw_data.drop(after_sep_data)
    testing_data = raw_data.drop(before_sep_data)

    return raw_data, training_data, testing_data

def generateNewsTable(data, filename):

    data_news = data.drop_duplicates(['news_title'])
    length = len(data_news.index)
    print("The length of news is " + str(length))
    processes_news = [(data_news['news_id'].values[i],
                   preprocess_WithSpeech(data_news['news_title'].values[i]
                                         + "_" + data_news['news_content'].values[i]))
                  for i in range(0, length)]

    writeNewsInFile(processes_news, filename)


if __name__ == '__main__':
    filename = '/Users/luoyi/Documents/Python/RecommendationSystem/data/user_click_data.txt'
    sep = '\t'
    names = ['user_id', 'news_id', 'read_time', 'news_title', 'news_content', 'news_publi_time']
    raw_data, training_data, testing_data = get_data(filename, sep, names=names)
    # calculateNumber(training_data)
    # calculateNumber(testing_data)

    #Reading history
    #Generate user_id-news_id table
    # length = len(training_data.index)
    # uid_nid = training_data.loc[:,['user_id','news_id']].values
    # writeTwoIndexesInFile(uid_nid, "/Users/luoyi/Documents/Python/RecommendationSystem/data/uid_nid.txt")

    #Divide users in test set into new_user and old_user with known reading history
    # test_user_id = set(list(testing_data['user_id'].values))
    # tran_user_id = set(list(training_data['user_id'].values))
    #
    # new_user_test = test_user_id - tran_user_id
    # old_user_test = test_user_id - new_user_test
    # writeIntegerInFile(new_user_test, "/Users/luoyi/Documents/Python/RecommendationSystem/data/new_user_test.txt")
    # writeIntegerInFile(old_user_test, "/Users/luoyi/Documents/Python/RecommendationSystem/data/old_user_test.txt")

    # generate news_id-news_content table of training data
    generateNewsTable(training_data, "/Users/luoyi/Documents/Python/RecommendationSystem/data/training_data.txt")
    generateNewsTable(testing_data, "/Users/luoyi/Documents/Python/RecommendationSystem/data/testing_data.txt")
