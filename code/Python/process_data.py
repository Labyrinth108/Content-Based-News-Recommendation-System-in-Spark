import pandas as pd
import time
import numpy as np
from utils import *
import os

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

def generateNewsTable(stopword_file, data, filename):

    data_news = data.drop_duplicates(['news_title'])
    length = len(data_news.index)
    print("The length of news is " + str(length))

    #News(id, publish_time, content after segmented)
    processes_news = [(data_news['news_id'].values[i], data_news['news_publi_time'].values[i],
                   preprocess_WithSpeech(stopword_file, data_news['news_title'].values[i]
                                         + "_" + data_news['news_content'].values[i]))
                  for i in range(0, length)]

    writeNewsInFile(processes_news, filename)

def generateReadingRecord(data, filename):

    length = len(data.index)
    print(length)
    uid_nid = data.loc[:,['user_id','news_id', 'read_time']].values

    file = open(filename, 'w')
    for index in uid_nid:
        try:
            file.write(str(index[0]) + "," + str(index[1]) + "," + str(index[2]) + "\n")
        except Exception as e:
            print(index)
    file.close()

if __name__ == '__main__':
    dir = "/Users/luoyi/Documents/Python/Capstone_Project/Content-Based-News-Recommendation-System-in-Spark"
    filename = dir + '/data/user_click_data.txt'
    sep = '\t'
    names = ['user_id', 'news_id', 'read_time', 'news_title', 'news_content', 'news_publi_time']

    raw_data, training_data, testing_data = get_data(filename, sep, names=names)
    # calculateNumber(training_data)
    # calculateNumber(testing_data)

    # Training Set ---- Reading history of users
    # generateReadingRecord(training_data, dir + "/data/training_uid_nid.txt")
    #
    # # Divide users in test set into new_user and old_user with known reading history
    # test_user_id = set(list(testing_data['user_id'].values))
    # tran_user_id = set(list(training_data['user_id'].values))
    #
    # new_user_test = test_user_id - tran_user_id
    # old_user_test = test_user_id - new_user_test

    #record user_id in test dataset
    # writeIntegerInFile(new_user_test, dir + "/data/test_new_user_id.txt")
    # writeIntegerInFile(old_user_test, dir + "/data/test_old_user_id.txt")

    # Testing set ---- Reading history(uid, nid, read_time) of users
    # uid_nid = testing_data.loc[:,['user_id','news_id', 'read_time']].values
    #
    # old_user_file = open(dir + "/data/test_old_user_records.txt", "w")
    # new_user_file = open(dir + "/data/test_new_user_records.txt", "w")
    #
    # for (uid, nid, r_time) in uid_nid:
    #     if uid in old_user_test:
    #         old_user_file.write(str(uid) + "," + str(nid) + ","+ str(r_time) + "\n")
    #     else:
    #         new_user_file.write(str(uid) + "," + str(nid) + ","+ str(r_time) + "\n")
    # old_user_file.close()
    # new_user_file.close()


    # parsed news in training and test data
    stopword_file = dir + "/data/stopwords.txt"
    generateNewsTable(stopword_file, training_data, dir + "/data/training_data.txt")
    generateNewsTable(stopword_file, testing_data, dir + "/data/testing_data.txt")
