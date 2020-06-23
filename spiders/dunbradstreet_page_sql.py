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
        categoryID = data[0]
        pageID = data[1][0]
        townID = data[1][-1]
        url = DNB_BASE + data[1][2]
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
        limite = 1
    # for i in range(num_industry):
    while True:
        IndustryID = GetItemID(DB_NAME, "Industry", [i+1])
        category_datas = SelectItems(DB_NAME, "Category", "IndustryID,", [IndustryID,])
        numCategorys = len(category_datas)
        NewPageDatas = []
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
                company_datas = SelectItems(DB_NAME, "PageCompany", "CategoryID, TownID, PageID", [CategoryID, TownID, PageID])
                if (len(company_datas) == 0):
                    NewPageDatas.append([CategoryID, page_data])
        num_add = len(NewPageDatas)
        print (f"Industry {i+1:02d} Pages ... Number to add:\t{num_add}")
        Q = multiprocessing.Queue()
        P = multiprocessing.Process(target= run_dunbrad_spider, args= (NewPageDatas, Q))
        P.start()
        P.join(timeout= num_add * 2 if num_add > 10 else 20)
        PageCompanyData = []
        res = -1
        while not Q.empty():
            loc_data = Q.get(timeout= 5)
            res = loc_data["result"]
            townID = loc_data["town"]
            pageID = loc_data["page"]
            categoryID = loc_data["category"]
            page_companys = loc_data["data"]
            if res == 0:
                if len(page_companys) > 0:
                    for name in page_companys:
                        url = page_companys[name]["URL"]
                        salesRevenue = page_companys[name]["Sales"]
                        PageCompanyData.append((name, url, salesRevenue, categoryID, townID, pageID))
        if len(PageCompanyData) == 0:
            limite += 1
        print (f"Industry {i+1:02d} Page ... Num insert\t{len(PageCompanyData)}")
        Q.close()
        Q.join_thread()
        P.kill()
        INSERT(DB_NAME, "PageCompany", PageCompanyData)
        if num_add == 0:
            break