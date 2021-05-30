import logging
import datetime as dt
import os
import scrapy
import sys
# import from crochet import setup
from twisted.internet import reactor
from scrapy.crawler import CrawlerProcess, CrawlerRunner
sys.path.append('.') 
import utils.configs_for_code as cfg


def main():
    logging.info('Start Web Crawler.')

    # define spider
    class HashtagSpider(scrapy.Spider):
        name = "umfrageergebnisse"

        def start_requests(self):
            urls = [
                'https://getdaytrends.com/germany/'
            ]
            for url in urls:
                yield scrapy.Request(url=url, callback=self.parse)

        def parse(self, response):
            hashtags_fname = cfg.PATH_HASHTAGS_FILE

            # set defaults
            sep = ','

            # crawl the actual data
            web_info = response.xpath('//td[@class="main"]//text()').getall()
            hashtags = []
            for idx, hashtag in enumerate(web_info):
                if idx%2 == 0: hashtags.append(hashtag)

            # write data
            hashtag_fcontent = sep.join(hashtags)
            with open(hashtags_fname, "w") as f:
                f.write(hashtag_fcontent)

            self.log('Saved file %s' % hashtags_fname)


        def clean_umlaute(self, input):
            replacers = {'ä': 'ae', 'ö': 'oe',
                        'ü': 'ue', 'ß': 'ss',
                        'Ä': 'AE', 'Ö': 'OE',
                        'Ü': 'UE', '–': '-'
                        }
            for key, value in replacers.items():
                input = input.replace(key, value)

            return input


    process = None
    process = CrawlerRunner()
    crawler = process.crawl(HashtagSpider)

    crawler.addBoth(lambda _: reactor.stop())
    reactor.run() # the script will block here until the crawling is finished

if __name__ == "__main__":
    main()

    

    ###### useful sql snippets #####

    # cursor.execute("""insert into sonntagsfrage.test_poc(
    #     test, bla, datum, ts) values (
    #     'pyodbc', 'awesome library', """ + dt.datetime.date() + """, """ dt.datetime.now() """
    #     )""")

    #commit the transaction
    # conn.commit()

    # insert rows into Azure SQL DB
    # for row in result:
    #     insertSql = "insert into TableName (Col1, Col2, Col3) values (?, ?, ?)"
    #     cursor.execute(insertSql, row[0], row[1], row[2])
    #     cursor.commit()
        
    # snippet for selecting data from azure sql
    # row = cursor.fetchone()
    # while row:
    #     print (str(row[0]) + " " + str(row[1]))
    #     row = cursor.fetchone()

    #     MERGE Production.UnitMeasure AS target  
    # USING (SELECT @UnitMeasureCode, @Name) AS source (UnitMeasureCode, Name)  
    # ON (target.UnitMeasureCode = source.UnitMeasureCode)  
    # WHEN MATCHED THEN
    #     UPDATE SET Name = source.Name  
    # WHEN NOT MATCHED THEN  
    #     INSERT (UnitMeasureCode, Name)  
    #     VALUES (source.UnitMeasureCode, source.Name)  