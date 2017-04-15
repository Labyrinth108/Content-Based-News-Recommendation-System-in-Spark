# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin
from models import News, Users, Records
# Register your models here.

class News_admin(admin.ModelAdmin):
    list_display = ('news_id', 'title', 'pub_date', 'content')

class Records_admin(admin.ModelAdmin):
    list_display = ('news_id', 'user_id', 'read_time')

admin.site.register(News, News_admin)
admin.site.register(Users)
admin.site.register(Records, Records_admin)
