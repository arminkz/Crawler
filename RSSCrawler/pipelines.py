# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy import signals
from scrapy.contrib.exporter import CsvItemExporter
from scrapy.exceptions import DropItem

from termcolor import colored
import csv

class NewsCrawlerPipeline(object):

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        self.file = open('output.csv', 'w+b')
        self.exporter = CsvItemExporter(self.file)
        self.exporter.start_exporting()

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

class NewsCrawlerCsvPipeline(object):

    def open_spider(self, spider):
        print(colored('[PIPELINE]','yellow'),'Opened')
        self.file = open("crawled.csv","a+",encoding='utf-8')

    def close_spider(self, spider):
        print(colored('[PIPELINE]', 'yellow'), 'Close')
        self.file.close()

    def process_item(self, item, spider):
        with open("crawled.csv", "r") as f:
            csvreader = csv.reader(f, delimiter=",")
            next(csvreader, None) #skip headers
            for row in csvreader:
                if item['link'] == row[6]:
                    print(colored('[SKIP]', 'red'), 'Duplicate Item.')
                    raise DropItem("Duplicate Item.")

        print(colored('[ADD]', 'blue'), 'Adding Item ...')

        fieldnames = ['date','title','summary','content','thumbnail','source','link','tags']
        writer = csv.DictWriter(self.file, fieldnames=fieldnames)
        if self.file.tell() == 0:
            writer.writeheader()
        writer.writerow(item)
        return item
