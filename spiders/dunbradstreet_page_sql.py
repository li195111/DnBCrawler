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
TB_NAME = "PageCompany"
class CategoryPage(scrapy.Spider):

    name = "category_spider"
    base_url = 'https://www.dnb.com'
    start_urls = []
    q = None
    CategoryID = None
    TownID = None
    PageID = None
    PageCompanys = {}
    def parse(self, response):
        # print('\n********** Second Status Info **********\n')
        if response.status == 200:
            print(f"URL :\t\t{response.url} ... OK !")
            load_region_info = response.css("h1.title::text")
            total_companys = response.xpath("//div[@id='companyResults']").css("div.data")
            load_error = ('Oh no! 500 Error' in load_region_info.extract() or 'Oh no! 404 Error' in load_region_info.extract())
            result = 0
            if not load_error and (self.start_urls[0] == response.url):
                for company in total_companys:
                    name = company.css("div.col-md-6").css("a::text").extract_first().strip()
                    url = company.css("div.col-md-6").css("a::attr('href')").extract_first()
                    salesRevenue = ''.join([sale.strip() for sale in company.css("div.col-md-2::text").extract() if len(sale.strip()) != 0])
                    salesRevenue = salesRevenue if len(salesRevenue) > 0 else '-'
                    self.PageCompanys[name] = {"URL":url,"Sales":salesRevenue}
            else:
                result = -1
                print (f"Error {self.start_urls[0]}")
            out = {"result":result,"data":self.PageCompanys,"category":self.CategoryID,"town":self.TownID,"page":self.PageID}
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
        pageID = data[0]
        url = DNB_BASE + data[2]
        categoryID = data[3]
        townID = data[4]
        process.crawl(CategoryPage, start_urls= [url,], q= Q, CategoryID= categoryID, TownID= townID, PageID= pageID)
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
        i = 0
    if len(sys.argv) > 2:
        limite = int(sys.argv[2])
    else:
        limite = 20
    max_iter = 10
    it = 1
    while True:
        total = 0
        parse = 0
        # for i in range(5):
        IndustryID = GetItemID(DB_NAME, "Industry", [i+1])
        category_datas = SelectItems(DB_NAME, "Category", "IndustryID,", [IndustryID,])
        numCategorys = len(category_datas)
        categorys = 0
        categroy_parse = 0
        for category_idx, category_data in enumerate(category_datas):
            Q = multiprocessing.Queue()
            jobs = []
            CategoryID = category_data[0]
            page_datas = SelectItems(DB_NAME, "Page", "CategoryID,", [CategoryID,])
            num_page_datas = len(page_datas)
            categorys += num_page_datas
            for page_idx, page_data in enumerate(page_datas):
                PageID = page_data[0]
                TownID = page_data[-1]
                company_datas = SelectItems(DB_NAME, TB_NAME, "CategoryID, TownID, PageID", [CategoryID, TownID, PageID])
                print (f"{str(page_data[1]):50s}:\t{(page_idx+1)*100/num_page_datas:.2f} %",end= '\r')
                if (len(company_datas) == 0):
                    P = multiprocessing.Process(target= run_dunbrad_spider, args= ([page_data,], Q))
                    P.start()
                    jobs.append(P)
                    time.sleep(0.8)
                num_jobs = len(jobs)
                if num_jobs >= limite:
                    categroy_parse += num_jobs
                    jobs, Q = do_jobs(DB_NAME, TB_NAME, i, category_idx, numCategorys, jobs, Q, num_page_datas)
            categroy_parse += num_jobs
            jobs, Q = do_jobs(DB_NAME, TB_NAME, i, category_idx, numCategorys, jobs, Q, num_page_datas)
        total += categorys
        parse += (categorys - categroy_parse)
        print (f"{it:03d} Total:\t{parse * 100 / (total+1e-8):.2f} %")
        if round(parse / (total+1e-8),0) == 1 or it >= max_iter:
            break
        it += 1
