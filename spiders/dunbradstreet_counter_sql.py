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
from utils import GetItemID, SelectItems, INSERT, INIT_DB

DNB_BASE = 'https://www.dnb.com'
DB_NAME = "test"

class CategoryPage(scrapy.Spider):

    name = "category_spider"
    base_url = 'https://www.dnb.com'
    start_urls = []
    q = None
    CategoryID = None
    CategoryName = None
    def parse(self, response):
        # print('\n********** Second Status Info **********\n')
        if response.status == 200:
            print(f"URL :\t\t{response.url} ... OK")
            RegionsNumb = []
            load_region_info = response.css("h1.title::text")
            load_error = 'Oh no! 500 Error' in load_region_info.extract() or 'Oh no! 404 Error' in load_region_info.extract()
            if not load_error:
                All_Regions = response.xpath("//div[@class='industry_country_crawl parbase basecomp section']").css("div.container").css("div.col-xs-6")
                for region in All_Regions:
                    Name = region.css("a::text").extract_first().strip()
                    Url = region.css("a::attr('href')").extract_first()
                    Number = region.css("a").css("span.number-countries::text").extract_first().replace("(","").replace(")","").replace(",","")
                    try:
                        Number = int(Number)
                    except ValueError:
                        Number = 0
                    RegionsNumb.append(Number)
            else:
                next_page = -1
                print (f"Error {self.start_urls[0]}")
            self.q.put({"ID":self.CategoryID,"Name":self.CategoryName,"data":sum(RegionsNumb)})
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
        categoryName = data[1]
        url = DNB_BASE + data[2]
        process.crawl(CategoryPage, start_urls= [url,], q= q, CategoryID= categoryID, CategoryName= categoryName)
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
    all_parses = 0
    parsesed = 0
    num_industry = len(ALL_INDUSTRY)
    while True:
        for i in range(num_industry):
        # if len(sys.argv) > 1:
        #     i = int(sys.argv[1])
        # else:
        #     i = 1
            # if len(sys.argv) > 2:
            #     limite = int(sys.argv[2])
            # else:
            #     limite = 10
            IndustryID = GetItemID(DB_NAME, "Industry", [i+1])
            category_datas = SelectItems(DB_NAME, "Category", "IndustryID,", [IndustryID,])
            num_category = len(category_datas)
            
            Q = multiprocessing.Queue()
            jobs = []
            for data in category_datas:
                item = SelectItems(DB_NAME, "NumberOfCompany", "CategoryID,",[data[0],])
                if len(item) == 0:
                    P = multiprocessing.Process(target= run_dunbrad_spider, args= ([data,], Q))
                    P.start()
                    jobs.append(P)
                    time.sleep(0.8)
            num_jobs = len(jobs)
            print (f"Industry {i+1}\t... Number to add:\t{num_jobs*100/num_category:.2f} % Parse Rate:\t{(num_category-num_jobs)*100/num_category:.2f} %")
            for P in jobs:
                P.join(timeout= 20)
            CountDatas = []
            while not Q.empty():
                region_datas = Q.get(timeout= 5)
                categoryID = region_datas["ID"]
                categoryName = region_datas["Name"]
                NumberOfCompany = region_datas["data"]
                if NumberOfCompany != 0:
                    CountDatas.append((categoryName, NumberOfCompany, categoryID, IndustryID))
                # print (f"{categoryID} {categoryName}:\t{NumberOfCompany}")
            INSERT(DB_NAME, "NumberOfCompany", CountDatas)
            Q.close()
            Q.join_thread()
            for P in jobs:
                P.kill()
            
        if num_jobs == 0:
            break