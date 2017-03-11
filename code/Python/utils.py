# -*- coding: UTF-8 -*-
import jieba
import io, re
import jieba.analyse, jieba.posseg
import time

def calculateNumber(df):
    print(len(df.index))

def writeInFile(content, filename):
    file = io.open(filename, 'w', encoding="utf-8")
    for news in content:
        try:
            file.write(news + "\n")
        except Exception as e:
            print(news)
    file.close()

def writeIntegerInFile(content, filename):
    file = open(filename, 'w')
    for news in content:
        try:
            file.write(str(news) + "\n")
        except Exception as e:
            print(news)
    file.close()

def writeNewsInFile(content, filename):
    file = io.open(filename, 'w', encoding="utf-8")
    for news in content:
        try:
            # 2014年03月28日16:55 "%Y-%m-%d %H:%M"
            time_str = news[1].decode("utf-8")
            time_ = time_str[:4] + "-" + time_str[5:7] + "-" + time_str[8:10]
            if len(time_str) > 11:
                time_ = time_ + " " + time_str[11:13] + ":" + time_str[14:16]
                time_unix = time.mktime(time.strptime(time_, "%Y-%m-%d %H:%M"))
            else:
                time_unix = time.mktime(time.strptime(time_, "%Y-%m-%d"))

            file.write(str(news[0]) + "," + news[2] + "," + str(time_unix) + "\n")
        except Exception as e:
            print(news)
    file.close()


def hasNumbersAlphas(inputString):
    if re.search("\w", inputString):
        return True
    return False

def hasChineseNumbers(intputString):
    ChineseNumbers = ['一','二','三','四','五','六','七','八','九','十']
    return any(number in intputString for number in ChineseNumbers)

def preprocess(stopword_file, news):
    content = jieba.cut(news, cut_all=False)  # 精确模式分词
    content = filter(lambda x: len(x) > 1, content)  # 过滤掉长度为1 的词（排除干扰）
    content = filter(lambda x: not hasChineseNumbers(x.encode("utf-8")), content)
    content = filter(lambda x: not hasNumbersAlphas(x), content)  # 过滤掉含有数字和字母的词

    stopw = [line.strip().decode('utf-8') for line in open(stopword_file).readlines()]

    parsed = set(content) - set(stopw)
    return ' '.join(parsed)

def hasMeaningfulWords(x):
    flag = x.flag
    target_set = ['n','nt', 'nz', # nt--机构团体名, nz=其他专有名
                  'a','ad','an','d',
                  'v','vd','vn','vi','vl']

    if flag in target_set:
        return True
    return False

def preprocess_WithSpeech(stopword_file, news):
    content = jieba.posseg.cut(news)  # 精确模式分词

    content = filter(lambda x: hasMeaningfulWords(x), content)  # 留下名词，形容词，副词
    content = [i.word for i in content]
    content = filter(lambda x: len(x) > 1, content)  # 过滤掉单字

    stopw = [line.strip().decode('utf-8') for line in open(stopword_file).readlines()]

    parsed = set(content) - set(stopw)
    return ' '.join(parsed)