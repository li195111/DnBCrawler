import numpy as np
import multiprocessing
import sys
import traceback
from scrapy.crawler import CrawlerProcess
import scrapy
import logging
logging.getLogger('scrapy').setLevel(logging.WARNING)
logging.getLogger('scrapy').propagate = False
import time
import json
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import GetItemID, SelectItems, INSERT, INIT_DB, do_jobs

DNB_BASE = 'https://www.dnb.com'
DB_NAME = "test"
TB_NAME = "Region"
class CategoryPage(scrapy.Spider):

    name = "category_spider"
    base_url = 'https://www.dnb.com'
    start_urls = []
    q = None
    CategoryID = None
    def parse(self, response):
        # print('\n********** Second Status Info **********\n')
        if response.status == 200:
            print(f"URL :\t\t{response.url} ... OK")
            RegionsDict = {}
            load_region_info = response.css("h1.title::text")
            load_error = 'Oh no! 500 Error' in load_region_info.extract() or 'Oh no! 404 Error' in load_region_info.extract()
            if not load_error:
                All_Regions = response.xpath("//div[@class='industry_country_crawl parbase basecomp section']").css("div.container").css("div.col-xs-6")
                for region in All_Regions:
                    Name = region.css("a::text").extract_first().strip()
                    Url = region.css("a::attr('href')").extract_first()
                    RegionsDict[Name] = Url
            else:
                next_page = -1
                print (f"Error {self.start_urls[0]}")
            self.q.put({"category":self.CategoryID,"data":RegionsDict})
        # print('\n********** Second Status Info **********\n')


def run_dunbrad_spider(industrys, q):
    process = CrawlerProcess(
            {
                'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
                'ROBOTSTXT_OBEY': False,
                'DOWNLOAD_DELAY': 3,
                'CONCURRENT_REQUESTS': 16,
                'TELNETCONSOLE_PORT' : None,
                'TELNETCONSOLE_ENABLED':False
            }
        )
    for data in industrys:
        categoryID = data[0]
        url = DNB_BASE + data[2]
        process.crawl(CategoryPage, start_urls= [url,], q= q, CategoryID= categoryID)
    process.start()
    
def Hello(url, q):
    print (f"Hello ... {url}")
    return 

specific_industry_names = np.array(["Manufacturing Sector",
                                    "Membership Organizations",
                                    "Mining",
                                    "Nonresidential Building Construction",
                                    "Wholesale Sector",
                                    "Heavy & Civil Engineering Construction",
                                    "Lodging",
                                    "Residential Construction Contractors",
                                    "Specialty Contractors",
                                    "Agriculture & Forestry Sector",
                                    "Professional Services Sector",
                                    "Rental & Leasing",
                                    "Retail Sector",
                                    "Business Services Sector",
                                    "Education Sector",
                                    "Electric Utilities",
                                    "Finance & Insurance Sector",
                                    "Real Estate",
                                    "Religious Organizations",
                                    "Arts, Entertainment & Recreation Sector",
                                    "Consumer Services",
                                    "Government",
                                    "Health Care Sector",
                                    "Management of Companies & Enterprises",
                                    "Media",
                                    "Natural Gas Distribution & Marketing",
                                    "Nonprofit Institutions",
                                    "Oil & Gas Exploration & Production",
                                    "Oil & Gas Field Services",
                                    "Oil & Gas Well Drilling",
                                    "Private Households",
                                    "Restaurants, Bars & Food Services",
                                    "Transportation Services Sector",
                                    "Water & Sewer Utilities"])

if __name__ == "__main__":
    CUR_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    INDUSTRY_DIR = os.path.join(CUR_PATH,"Industrys")
    ALL_INDUSTRY = os.listdir(INDUSTRY_DIR)
    INIT_DB(DB_NAME)
    num_industry = len(ALL_INDUSTRY)
    max_iter = 10
    it = 1
    if len(sys.argv) > 1:
        i = int(sys.argv[1])
    else:
        i = 2
    while True:
        total = 0
        parse = 0
        # for i in range(5):
        IndustryID = GetItemID(DB_NAME, "Industry", [i+1])
        category_datas = SelectItems(DB_NAME, "Category", "IndustryID,", [IndustryID,])
        num_category = len(category_datas)
        total += num_category
        Q = multiprocessing.Queue()
        jobs = []
        for idx, data in enumerate(category_datas):
            region_datas = SelectItems(DB_NAME, TB_NAME, "CategoryID,", [data[0],])
            if (len(region_datas) == 0):
                P = multiprocessing.Process(target= run_dunbrad_spider, args= ([data,], Q))
                P.start()
                jobs.append(P)
                time.sleep(0.8)
        num_jobs = len(jobs)
        parse += (num_category - num_jobs)
        jobs, Q = do_jobs(DB_NAME, TB_NAME, i, idx, num_category, jobs, Q, idx, num_category)
        print (f"{it:03d} Total:\t{parse * 100 / (total+1e-8):.2f} %")
        if round(parse / (total+1e-8),0) >= 0.95 or it >= max_iter:
            break
        it += 1