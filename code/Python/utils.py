# -*- coding: UTF-8 -*-
import jieba
import io, re
import jieba.analyse, jieba.posseg

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

def hasNumbersAlphas(inputString):
    if re.search("\w", inputString):
        return True
    return False

def hasChineseNumbers(intputString):
    ChineseNumbers = ['一','二','三','四','五','六','七','八','九','十']
    return any(number in intputString for number in ChineseNumbers)

def preprocess(news):
    content = jieba.cut(news, cut_all=False)  # 精确模式分词
    content = filter(lambda x: len(x) > 1, content)  # 过滤掉长度为1 的词（排除干扰）
    content = filter(lambda x: not hasChineseNumbers(x.encode("utf-8")), content)
    content = filter(lambda x: not hasNumbersAlphas(x), content)  # 过滤掉含有数字和字母的词

    stopw = [line.strip().decode('utf-8') for line in open('/Users/luoyi/Documents/Python/RecommendationSystem/data/stopwords.txt').readlines()]

    parsed = set(content) - set(stopw)
    return ' '.join(parsed)

def hasMeaningfulWords(x):
    flag = x.flag
    target_set = ['n', 'ns', 'ni','nh','nt', 'nz','a','ad','an','d','v','vd','vn','vi','vl']

    if flag in target_set:
        return True
    return False

def preprocess_WithSpeech(news):
    content = jieba.posseg.cut(news)  # 精确模式分词

    content = filter(lambda x: hasMeaningfulWords(x), content)  # 留下名词，形容词，副词
    content = [i.word for i in content]
    content = filter(lambda x: len(x) > 1, content)  # 过滤掉单字

    stopw = [line.strip().decode('utf-8') for line in open('/Users/luoyi/Documents/Python/RecommendationSystem/data/stopwords.txt').readlines()]

    parsed = set(content) - set(stopw)
    return ' '.join(parsed)