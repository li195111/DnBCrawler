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
TB_NAME = "Company"
class CategoryPage(scrapy.Spider):

    name = "category_spider"
    base_url = 'https://www.dnb.com'
    start_urls = []
    q = None
    CategoryID = None
    TownID = None
    PageCompanyID = None
    SalesRevenue = None
    def parse(self, response):
        # print('\n********** Second Status Info **********\n')
        if response.status == 200:
            print(f"URL :\t\t{response.url} ... OK !")
            load_region_info = response.css("h1.title::text")
            company_info = response.xpath("//div[@class='tile hero']").css("div.col-md-12")
            load_error = ('Oh no! 500 Error' in load_region_info.extract() or 'Oh no! 404 Error' in load_region_info.extract())
            result = 0
            Company = {}
            if not load_error and (len(self.start_urls[0]) == len(response.url)):
                Name = company_info.css("h1.title::text").extract_first()
                tradeName = company_info.css("div.tradeName::text").extract_first()
                Address = [add.strip() for add in company_info.css("div.company_street_address").css("::text").extract() if len(add.strip()) != 0]
                Location = [loc.strip() for loc in company_info.css("div.company_regional_address").css("::text").extract() if len(loc.strip()) != 0]
                ComType = company_info.css("div.type-role").css("span.type::text").extract_first()
                ComType = ComType if not ComType is None else '-'
                ComRole = company_info.css("div.type-role").css("span.role::text").extract_first()
                ComRole = ComRole if not ComRole is None else '-'
                Website = company_info.css("div.web").css("a::attr('href')").extract_first()
                Phone = [phone.strip() for phone in company_info.css("div.phone::text").extract()]

                Company["Name"] = Name if not Name is None else '-'
                Company["Trade"] = tradeName if not tradeName is None else '-'
                Company["Address"] = ''.join(Address) if not len(Address) == 0 else '-'
                Company["Location"] = ''.join(Location) if not len(Location) == 0 else '-'
                Company["CompanyType"] = ComType + ComRole
                Company["WebSite"] = Website if not Website is None else '-'
                Company["Phone"] = Phone[0] if len(Phone) > 0 else '-'
            else:
                result = -1
                print (f"Error {self.start_urls[0]}")
            out = {"result":result,"data":Company,"category":self.CategoryID,"town":self.TownID,"pagecompany":self.PageCompanyID,"sales":self.SalesRevenue}
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
        pagecompanyID = data[0]
        url = DNB_BASE + data[2]
        salesRevenue = data[3]
        categoryID = data[4]
        townID = data[5]
        process.crawl(CategoryPage, start_urls= [url,], q= Q, CategoryID= categoryID, TownID= townID, PageCompanyID= pagecompanyID, SalesRevenue= salesRevenue)
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
            pagecompany_datas = SelectItems(DB_NAME, "PageCompany", "CategoryID,", [CategoryID,])
            num_pagecompany_datas = len(pagecompany_datas)
            categorys += num_pagecompany_datas
            for page_idx, pagecompany_data in enumerate(pagecompany_datas):
                PageCompanyID = pagecompany_data[0]
                TownID = pagecompany_data[-2]
                company_datas = SelectItems(DB_NAME, TB_NAME, "CategoryID, TownID, PageCompanyID", [CategoryID, TownID, PageCompanyID])
                print (f"{pagecompany_data[1]:50s}:\t{(page_idx+1)*100/num_pagecompany_datas:.2f} %",end= '\r')
                if (len(company_datas) == 0):
                    P = multiprocessing.Process(target= run_dunbrad_spider, args= ([pagecompany_data,], Q))
                    P.start()
                    jobs.append(P)
                    time.sleep(1)
                num_jobs = len(jobs)
                if num_jobs >= limite:
                    categroy_parse += num_jobs
                    jobs, Q = do_jobs(DB_NAME, TB_NAME, i, category_idx, numCategorys, jobs, Q, num_pagecompany_datas)
            categroy_parse += num_jobs
            jobs, Q = do_jobs(DB_NAME, TB_NAME, i, category_idx, numCategorys, jobs, Q, num_pagecompany_datas)
        total += categorys
        parse += (categorys - categroy_parse)
        print (f"{it:03d} Total:\t{parse * 100 / (total+1e-8):.2f} %")
        if round(parse / (total+1e-8),0) >= 0.95 or it >= max_iter:
            break
        it += 1
