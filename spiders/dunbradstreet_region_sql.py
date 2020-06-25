import numpy as np
import multiprocessing
import sys
import traceback
from scrapy.crawler import CrawlerProcess, CrawlerRunner
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
TB_NAME = "Location"
class RegionPage(scrapy.Spider):

    name = "category_spider"
    base_url = 'https://www.dnb.com'
    start_urls = []
    q = None
    CategoryID = None
    RegionID = None
    RegionName = ""
    def parse(self, response):
        # print('\n********** Second Status Info **********\n')
        if response.status == 200:
            print(f"URL :\t\t{response.url} ... OK !")
            load_region_info = response.css("h1.title::text")
            locations = response.xpath("//div[@id='locationResults']").css("div.data")
            
            load_error = ('Oh no! 500 Error' in load_region_info.extract() or 'Oh no! 404 Error' in load_region_info.extract()) and (len(locations.extract()) == 0)
            next_page = 0
            region_locations = {}
            if not load_error and (self.start_urls[0] == response.url):
                if len(locations.extract()) > 0:
                    for loc in locations:
                        name = loc.css("a::text").extract_first().strip()
                        url = loc.css("a::attr('href')").extract_first()
                        # numb = loc.css("a").css("span.number-countries::text").extract_first().replace("(","").replace(")","").replace(",","")
                        region_locations[name] = url
                        # total_numb += int(numb)
                    # print (f"Number of Region Locations: {len(region_locations)}")
                else:
                    next_page = 1
                    region_locations = response.url.replace(self.base_url,"")
            else:
                next_page = -1
                print (f"Error {self.start_urls[0]}")
            out = {"result":next_page,"data":region_locations,"category":self.CategoryID,"region":self.RegionID, "region_name":self.RegionName}
            self.q.put(out)
        else:
            print(f"URL :\t\t{response.url} ... Error Code: {response.status}\n")
        # print('\n********** Second Status Info **********\n')
        return region_locations

def run_dunbrad_spider(regions, Q):
    process = CrawlerProcess(
            {
                'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
                # 'ROBOTSTXT_OBEY': False,
                'DOWNLOAD_DELAY': 1.5,
                'CONCURRENT_REQUESTS': 16,
                'TELNETCONSOLE_PORT' : None,
                'TELNETCONSOLE_ENABLED':False
            }
        )
    for data in regions:
        regionID = data[0]
        regionName = data[1]
        url = DNB_BASE + data[2]
        categoryID = data[3]
        process.crawl(RegionPage, start_urls= [url,], q= Q, CategoryID= categoryID, RegionID= regionID, RegionName= regionName)
    process.start()
    process.stop()
    
def Hello(url, q):
    print (f"Hello ... ")
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
    # if len(sys.argv) > 2:
    #     limite = int(sys.argv[2])
    # else:
    #     limite = 2
    limite = 50
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
            region_datas = SelectItems(DB_NAME, "Region", "CategoryID,", [CategoryID,])
            num_reg_datas = len(region_datas)
            categorys += num_reg_datas
            for reg_idx, region_data in enumerate(region_datas):
                RegionID = region_data[0]
                location_datas = SelectItems(DB_NAME, TB_NAME, "CategoryID, RegionID", [CategoryID, RegionID])
                print (f"{region_data[1]:50s}:\t{(reg_idx+1)*100/num_reg_datas:.2f} %",end= '\r')
                if (len(location_datas) == 0):
                    P = multiprocessing.Process(target= run_dunbrad_spider, args= ([region_data,], Q))
                    P.start()
                    jobs.append(P)
                    time.sleep(0.8)
                num_jobs = len(jobs)
                if num_jobs >= limite:
                    categroy_parse += num_jobs
                    jobs, Q = do_jobs(DB_NAME, TB_NAME, i, category_idx, numCategorys, jobs, Q, num_reg_datas)
            categroy_parse += num_jobs
            jobs, Q = do_jobs(DB_NAME, TB_NAME, i, category_idx, numCategorys, jobs, Q, num_reg_datas)
        total += categorys
        parse += (categorys - categroy_parse)
        print (f"{it:03d} Total:\t{parse * 100 / (total+1e-8):.2f} %")
        if (parse / total+1e-8) == 1 or it >= max_iter:
            break
        it += 1