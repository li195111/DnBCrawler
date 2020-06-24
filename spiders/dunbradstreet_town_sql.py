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
from utils import GetItemID, SelectItems, INSERT, INIT_DB, do_jobs

DNB_BASE = 'https://www.dnb.com'
DB_NAME = "test"
TB_NAME = "Page"

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
                    try:
                        max_page = int(companys_info[2])
                    except ValueError:
                        max_page = current_page
                    max_display_company = int(companys_info[3])
                    for i in range(max_page):
                        town_pages[i+1] = response.url[:-1].replace(self.base_url,"") + str(i+1)
                else:
                    next_page = 1
                    town_pages = response.url.replace(self.base_url,"")
                pass
            else:
                next_page = -1
                print (f"Error {self.start_urls[0]}")
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
                'DOWNLOAD_DELAY': 1.5,
                'CONCURRENT_REQUESTS': 16,
                'TELNETCONSOLE_PORT' : None,
                'TELNETCONSOLE_ENABLED': False,
            }
        )
    for data in town_datas:
        townID = data[0]
        townName = data[1]
        url = DNB_BASE + data[2]
        categoryID = data[3]
        process.crawl(CategoryPage, start_urls= [url,], q= Q, CategoryID= categoryID, TownID= townID, TownName= townName)
    process.start()
    process.stop()
    
def Hello(url, q):
    print (f"Hello ... {url}")
    return 

if __name__ == "__main__":
    CUR_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    INDUSTRY_DIR = os.path.join(CUR_PATH,"Industrys")
    ALL_INDUSTRY = os.listdir(INDUSTRY_DIR)
    INIT_DB(DB_NAME)
    num_industry = len(ALL_INDUSTRY)
    # if len(sys.argv) > 1:
    #     i = int(sys.argv[1])
    # else:
    #     i = 2
    # if len(sys.argv) > 2:
    #     limite = int(sys.argv[2])
    # else:
    #     limite = 10
    limite = 20
    max_iter = 10
    it = 1
    while True:
        total = 0
        parse = 0
        for i in range(num_industry):
            IndustryID = GetItemID(DB_NAME, "Industry", [i+1])
            category_datas = SelectItems(DB_NAME, "Category", "IndustryID,", [IndustryID,])
            numCategorys = len(category_datas)
            categorys = 0
            categroy_parse = 0
            for idx, category_data in enumerate(category_datas):
                Q = multiprocessing.Queue()
                jobs = []
                CategoryID = category_data[0]
                town_datas = SelectItems(DB_NAME, "Town", "CategoryID,", [CategoryID,])
                num_town_datas = len(town_datas)
                for town_idx, town_data in enumerate(town_datas):
                    TownID = town_data[0]
                    page_datas = SelectItems(DB_NAME, TB_NAME, "CategoryID, TownID", [CategoryID, TownID])
                    if (len(page_datas) == 0):
                        P = multiprocessing.Process(target= run_dunbrad_spider, args= ([town_data,], Q))
                        P.start()
                        jobs.append(P)
                        time.sleep(0.8)
                    num_jobs = len(jobs)
                    if num_jobs >= limite:
                        categroy_parse += (num_town_datas - num_jobs)
                        jobs, Q = do_jobs(DB_NAME, TB_NAME, i, jobs, Q, num_town_datas)
                categroy_parse += (num_town_datas - num_jobs)
                jobs, Q = do_jobs(DB_NAME, TB_NAME, i, jobs, Q, num_town_datas)
            total += categorys
            parse += categroy_parse
        print (f"{it:03d} Total:\t{parse * 100 / total:.2f} %")
        if (parse / total) == 1 or it >= max_iter:
            break
        it += 1
