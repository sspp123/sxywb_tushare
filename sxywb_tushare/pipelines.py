# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from sxywb_tushare.config import *
from sxywb_tushare.db_utils import *
import logging
from logging.config import fileConfig
from sxywb_tushare.db_utils import *

class SxywbTusharePipeline(object):
    def process_item(self, item, spider):


            # 检查是否存在重复的通告
        if check_dup_record(item):
            logging.info('%s:%s已存在数据库中，忽略...' % (item.get('id'), item.get('title')))
            return;
        logging.info('新发现公告%s' % item.get('id'))
        if upsert_to_mongo({'id': item.get('id')}, item):
            logging.info('更新/插入[%s]成功' % item.get('id'))
        return item