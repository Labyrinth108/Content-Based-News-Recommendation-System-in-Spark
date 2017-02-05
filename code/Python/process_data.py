import pandas as pd
import time
import numpy as np

def changeToTimestamp(sep_time):
    time_struct = time.strptime(sep_time, "%Y-%m-%d %H:%M:%S")
    timestamp = int(time.mktime(time_struct))
    return timestamp

def get_data(filename, sep, names, septime = "2014-03-20 23:59:00"):
    raw_data = pd.read_table(filename, sep='\t', header=None, names=names).dropna(how='any')

    read_times = raw_data['read_time']
    sep_time = "2014-03-20 23:59:00" # the time dividing train data and test data
    timestamp = changeToTimestamp(sep_time)

    index_train_data = read_times.index[read_times < timestamp]
    index_test_data = read_times.index[read_times >= timestamp]

    training_data = raw_data.drop(index_train_data)
    testing_data = raw_data.drop(index_test_data)

    return raw_data, training_data, testing_data

if __name__ == '__main__':
    filename = '/Users/luoyi/Documents/Python/RecommendationSystem/data/user_click_data.txt'
    sep = '\t'
    names = ['user_id', 'news_id', 'read_time', 'news_title', 'news_content', 'news_publi_time']
    raw_data, training_data, testing_data = get_data(filename, sep, names=names)

    test_user_id = set(list(testing_data['user_id'].values))
    tran_user_id = set(list(training_data['user_id'].values))

    new_user_test = test_user_id - tran_user_id
    old_user_test = test_user_id - new_user_test
    np.save('/Users/luoyi/Documents/Python/RecommendationSystem/data/test_user_all_id.npy', test_user_id)
    np.save('/Users/luoyi/Documents/Python/RecommendationSystem/data/test_old_user_id.npy', old_user_test)
    np.save('/Users/luoyi/Documents/Python/RecommendationSystem/data/test_new_user_id.npy', new_user_test)
