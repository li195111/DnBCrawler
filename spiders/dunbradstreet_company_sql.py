import numpy as np
import multiprocessing
import sys
import traceback
from scrapy.crawler import CrawlerProcess
import scrapy
import logging
logging.getLogger('scrapy').setLevel(logging.WARNING)
logging.getLogger('scrapy').propagate = False
import json
import os
import time 
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import GetItemID, SelectItems, INSERT, INIT_DB

DNB_BASE = 'https://www.dnb.com'
DB_NAME = "test"

class CategoryPage(scrapy.Spider):

    name = "category_spider"
    base_url = 'https://www.dnb.com'
    start_urls = []
    q = None
    CategoryID = None
    TownID = None
    TownName = ""
    def parse(self, response):
        # print('\n********** Second Status Info **********\n')
        if response.status == 200:
            print(f"URL :\t\t{response.url} ... OK !")
            load_region_info = response.css("h1.title::text")
            total_companys = response.xpath("//script/text()")
            load_error = ('Oh no! 500 Error' in load_region_info.extract() or 'Oh no! 404 Error' in load_region_info.extract())
            next_page = 0
            town_pages = {}
            if not load_error and (self.start_urls[0] == response.url):
                companys_info = []
                for script in total_companys:
                    script_text = script.extract()
                    if 'integrated_search.pagination' in script_text:
                        companys_info = script_text.split("(")[-1].split(")")[0].split(",")
                if len(companys_info) > 0:
                    current_page = int(companys_info[0])
                    max_numb_company = int(companys_info[1])
                    max_page = int(companys_info[2])
                    max_display_company = int(companys_info[3])
                    for i in range(max_page):
                        town_pages[i+1] = response.url[:-1].replace(self.base_url,"") + str(i+1)
                else:
                    next_page = 1
                    town_pages = response.url.replace(self.base_url,"")
                pass
            else:
                next_page = -1
                print (f"Error {response.url}")
            out = {"result":next_page,"data":town_pages,"category":self.CategoryID,"town":self.TownID,"town_name":self.TownName}
            self.q.put(out)
        else:
            print(f"URL :\t\t{response.url} ... Error Code: {response.status}\n")
        # print('\n********** Second Status Info **********\n')


def run_dunbrad_spider(town_datas, Q):
    process = CrawlerProcess(
            {
                'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
                # 'ROBOTSTXT_OBEY': False,
                'DOWNLOAD_DELAY': 1,
                'CONCURRENT_REQUESTS': 16,
                'TELNETCONSOLE_PORT' : None,
                'TELNETCONSOLE_ENABLED': False,
                # 'SPLASH_URL' : 'http://localhost:8050',
                # 'DOWNLOADER_MIDDLEWARES' : {'scrapy_splash.SplashCookiesMiddleware': 723,
                #                             'scrapy_splash.SplashMiddleware': 725,
                #                             'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 810,
                #                             },
                # 'SPIDER_MIDDLEWARES' : {'scrapy_splash.SplashDeduplicateArgsMiddleware': 100,
                # },
                # 'DUPEFILTER_CLASS' : 'scrapy_splash.SplashAwareDupeFilter',
                # 'HTTPCACHE_STORAGE' : 'scrapy_splash.SplashAwareFSCacheStorage',
            }
        )
    for data in town_datas:
        categoryID = data[0]
        pageID = data[1][0]
        townID = data[1][-1]
        url = DNB_BASE + data[1][2]
        process.crawl(CategoryPage, start_urls= [url,], q= Q, CategoryID= categoryID, TownID= townID, TownName= townName)
    process.start()
    # process.stop()
    
def Hello(url, q):
    print (f"Hello ... {url}")
    return 

if __name__ == "__main__":
    CUR_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    INDUSTRY_DIR = os.path.join(CUR_PATH,"Industrys")
    ALL_INDUSTRY = os.listdir(INDUSTRY_DIR)
    INIT_DB(DB_NAME)
    num_industry = len(ALL_INDUSTRY)
    if len(sys.argv) > 1:
        i = int(sys.argv[1])
    else:
        i = 3
    if len(sys.argv) > 2:
        limite = int(sys.argv[2])
    else:
        limite = 1
    # for i in range(num_industry):
    while True:
        IndustryID = GetItemID(DB_NAME, "Industry", [i+1])
        category_datas = SelectItems(DB_NAME, "Category", "IndustryID,", [IndustryID,])
        numCategorys = len(category_datas)
        NewPageDatas = []
        location_name = ""
        for idx, category_data in enumerate(category_datas):
            CategoryID = category_data[0]
            if len(NewPageDatas) >= limite:
                break
            page_datas = SelectItems(DB_NAME, "Page", "CategoryID,", [CategoryID,])
            num_page_datas = len(page_datas)
            for page_idx, page_data in enumerate(page_datas):
                PageID = page_data[0]
                TownID = page_data[-1]
                if len(NewPageDatas) >= limite:
                    break
                company_datas = SelectItems(DB_NAME, "Company", "CategoryID, TownID, PageID", [CategoryID, TownID, PageID])
                if (len(company_datas) == 0):
                    town_name = page_data[1]
                    NewPageDatas.append([CategoryID, page_data])
        num_add = len(NewPageDatas)
        print (f"Industry {i+1:02d} {location_name} ... Number to add:\t{num_add}")
        Q = multiprocessing.Queue()
        P = multiprocessing.Process(target= run_dunbrad_spider, args= (NewPageDatas, Q))
        P.start()
        P.join(timeout= num_add if num_add > 10 else 10)
        PageData = []
        res = -1
        while not Q.empty():
            loc_data = Q.get(timeout= 5)
            res = loc_data["result"]
            townID = loc_data["town"]
            townName = loc_data["town_name"]
            categoryID = loc_data["category"]
            town = loc_data["data"]
            if res == 0:
                if len(town) > 0:
                    for pagenumb in town:
                        url = town[pagenumb]
                        PageData.append((pagenumb, url, categoryID, townID))
            elif res == 1:
                url = town
                PageData.append((townName, url, categoryID, townID))
        print (f"Industry {i+1:02d} Page ... Num insert\t{len(PageData)}")
        INSERT(DB_NAME, "Page", PageData)
        if num_add == 0:
            break