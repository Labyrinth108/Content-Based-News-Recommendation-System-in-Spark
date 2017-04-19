# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models
from datetime import datetime
# Create your models here.
class News(models.Model):
    news_id = models.CharField(primary_key=True, max_length=20)
    title = models.CharField(max_length=200)
    pub_date = models.DateTimeField()
    content = models.TextField()
    def __str__(self):
        return str(self.news_id) + " " + self.title + " " + self.pub_date.strftime('%Y-%m-%d')

class Users(models.Model):
    user_id = models.CharField(primary_key=True, max_length=20)
    passwd = models.CharField('password',max_length=15)
    def __str__(self):
        return self.user_id

class Records(models.Model):
    news = models.ForeignKey('News')
    user = models.ForeignKey('Users')
    read_time = models.DateTimeField()

class TNews(models.Model):
    news_id = models.CharField(primary_key=True, max_length=20)
    title = models.CharField(max_length=200)
    pub_date = models.DateTimeField()
    content = models.TextField()
    def __str__(self):
        return str(self.news_id) + " " + self.title + " " + self.pub_date.strftime('%Y-%m-%d')

class RecommendSet(models.Model):
    user = models.ForeignKey('Users')
    news = models.ForeignKey('TNews')
    read_time = models.DateTimeField()
    read_ts = models.IntegerField()

class ClickRecords(models.Model):
    user = models.ForeignKey('Users')
    news = models.ForeignKey('TNews')
    read_time = models.DateTimeField()
    read_ts = models.IntegerField()