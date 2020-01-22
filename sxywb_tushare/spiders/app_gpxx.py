# -*- coding: utf-8 -*-
import json
import logging
import datetime
import time
import re
import scrapy
from scrapy import cmdline
from sxywb_tushare.db_utils import *
from sxywb_tushare.settings import USER_AGENT
from urllib.request import urlopen
from urllib.request import Request

from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import LTTextBoxHorizontal, LAParams
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfinterp import PDFTextExtractionNotAllowed
from pdfminer.pdfparser import PDFParser, PDFDocument

class AppGpxxSpider(scrapy.Spider):
    name = 'app_gpxx'
    allowed_domains = ['cninfo.com.cn']
    urls = ['http://www.cninfo.com.cn/new/commonUrl?url=disclosure/list/notice#szse/1']
    now_page=0
    now_page_hke = 0
    total_page =1
    hasmore ='true'
    curr_time =  datetime.datetime.now()
    curr_today= curr_time.strftime('%Y-%m-%d')
    curr_yesterday=(curr_time + datetime.timedelta(days=-1)).strftime('%Y-%m-%d')
    #print(curr_yesterday+'~'+curr_yesterday)
    def start_requests(self):
        #print(self.total_page)
           self.now_page+=1
           self.now_page_hke += 1
           logging.info("深泸现在爬取第{}页内容".format(self.now_page))

           logging.info("深泸开始爬取公告时间:" + self.curr_yesterday + '~' + self.curr_today)
           yield scrapy.FormRequest(
                url='http://www.cninfo.com.cn/new/hisAnnouncement/query',
                formdata={
                # 'infotypeId': '0',  # 这里不能给bool类型的True，requests模块中可以
                'column': 'szse',  # 这里不能给int类型的1，requests模块中可以
                'pageNum': str(self.now_page),
                'pageSize': '30',
                'tabName': 'fulltext',
                'plate': '',
                'stock': '',
                'searchkey': '',
                'secid': '',
                'category': '',
                'trade': '',
                'seDate': (self.curr_yesterday+'~'+self.curr_today),
                'sortName': '',
                'sortType': '',
                'isHLtitle': 'true',
                },  # 这里的formdata相当于requ模块中的data，key和value只能是键值对形式
                callback=self.parse
            )



    def parse(self, response):
      print(response.text)

      json_txt=  json.loads(response.text)
      totalAnnouncement=json_txt["totalAnnouncement"]
      announcements=json_txt["announcements"]
      self.hasmore =json_txt["hasMore"]
      for announcement in announcements:
          if check_id_mongo_jy(announcement) :
               logging.info("已经存在:"+announcement["announcementTitle"])
          else :

              pdflookurl='http://www.cninfo.com.cn/new/disclosure/detail?stockCode='+str(announcement["secCode"])+'&announcementId='+str(announcement["announcementId"])+'&orgId='+str(announcement["orgId"])+'&announcementTime='+str(announcement["announcementTime"])
              #print(pdflookurl)
              announcement["pdflookurl"]=pdflookurl
              announcement["exchange"]='深泸'
              url = 'http://static.cninfo.com.cn/'+announcement["adjunctUrl"]
              text = ''
              try:
                  text = convert_pdf_to_txt(url)
              except Exception as e:
                  logging.error("读取失败:" + pdflookurl + "标题:" + announcement["announcementTitle"] + ",错误:")
              if text:
                  p1 = r"\(cid:\d+\)+"
                  text = re.sub(p1, "", text)
              announcement["pdftxt"]=text
              announcement["id"]=announcement["announcementId"]

              save_to_mongo(announcement)
              time.sleep(0.5)
      self.total_page = totalAnnouncement//30 +1
      while self.now_page <= self.total_page :
          self.now_page += 1
          logging.info("深泸现在爬取第{}页内容".format(self.now_page))
          yield scrapy.FormRequest(
              url='http://www.cninfo.com.cn/new/hisAnnouncement/query',
              formdata={
                  # 'infotypeId': '0',  # 这里不能给bool类型的True，requests模块中可以
                  'column': 'szse',  # 这里不能给int类型的1，requests模块中可以
                  'pageNum': str(self.now_page),
                  'pageSize': '30',
                  'tabName': 'fulltext',
                  'plate': '',
                  'stock': '',
                  'searchkey': '',
                  'secid': '',
                  'category': '',
                  'trade': '',
                  'seDate': (self.curr_yesterday + '~' + self.curr_today),
                  'sortName': '',
                  'sortType': '',
                  'isHLtitle': 'true',
              },  # 这里的formdata相当于requ模块中的data，key和value只能是键值对形式
              callback=self.parse
          )


#pdf读取
def convert_pdf_to_txt(_path):
        # fp = open(_path, 'rb')  # rb以二进制读模式打开本地pdf文件
        request = Request(url=_path, headers={'User-Agent': USER_AGENT})  # 随机从user_agent列表中抽取一个元素
        fp = urlopen(request)  # 打开在线PDF文档

        # 用文件对象来创建一个pdf文档分析器
        praser_pdf = PDFParser(fp)

        # 创建一个PDF文档
        doc = PDFDocument()

        # 连接分析器 与文档对象
        praser_pdf.set_document(doc)
        doc.set_parser(praser_pdf)

        # 提供初始化密码doc.initialize("123456")
        # 如果没有密码 就创建一个空的字符串
        doc.initialize()

        # 检测文档是否提供txt转换，不提供就忽略
        if not doc.is_extractable:
            raise PDFTextExtractionNotAllowed
        else:
            # 创建PDf资源管理器 来管理共享资源
            rsrcmgr = PDFResourceManager()

            # 创建一个PDF参数分析器
            laparams = LAParams()

            # 创建聚合器
            device = PDFPageAggregator(rsrcmgr, laparams=laparams)

            # 创建一个PDF页面解释器对象
            interpreter = PDFPageInterpreter(rsrcmgr, device)

            # 循环遍历列表，每次处理一页的内容
            # doc.get_pages() 获取page列表
            str=''
            for page in doc.get_pages():
                # 使用页面解释器来读取
                interpreter.process_page(page)

                # 使用聚合器获取内容
                layout = device.get_result()

                # 这里layout是一个LTPage对象 里面存放着 这个page解析出的各种对象 一般包括LTTextBox, LTFigure, LTImage, LTTextBoxHorizontal 等等 想要获取文本就获得对象的text属性，
                for out in layout:
                    # 判断是否含有get_text()方法，图片之类的就没有
                    # if hasattr(out,"get_text"):
                    if isinstance(out, LTTextBoxHorizontal):
                        results = out.get_text()
                        str+=results
            return str
        fp.close()





if __name__ == "__main__":

    cmdline.execute("scrapy crawl app_gpxx".split())