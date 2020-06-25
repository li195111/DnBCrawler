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
TB_NAME = "Town"

class LocationPage(scrapy.Spider):

    name = "category_spider"
    base_url = 'https://www.dnb.com'
    start_urls = []
    q = None
    CategoryID = None
    LocationID = None
    LocationName = ""
    def parse(self, response):
        # print('\n********** Second Status Info **********\n')
        if response.status == 200:
            print(f"URL :\t\t{response.url} ... OK !")
            load_region_info = response.css("h1.title::text")
            towns = response.xpath("//div[@id='locationResults']").css("div.data")
            load_error = ('Oh no! 500 Error' in load_region_info.extract() or 'Oh no! 404 Error' in load_region_info.extract()) and (len(towns.extract()) == 0)
            next_page = 0
            location_towns = {}
            if not load_error and (self.start_urls[0] == response.url):
                if len(towns.extract()) > 0:
                    for town in towns:
                        name = town.css("a::text").extract_first().strip()
                        url = town.css("a::attr('href')").extract_first()
                        location_towns[name] = url
                else:
                    next_page = 1
                    location_towns = response.url.replace(self.base_url,"")
            else:
                next_page = -1
                print (f"Error {self.start_urls[0]}")
            # print (f"Number of Region Locations: {len(region_locations)}")
            out = {"result":next_page,"data":location_towns,"category":self.CategoryID,"location":self.LocationID,"location_name":self.LocationName}
            # print (f"Load Error: {load_error}, {next_page}, {len(location_towns)}")
            self.q.put(out)
        else:
            print(f"URL :\t\t{response.url} ... Error Code: {response.status}\n")
        # print('\n********** Second Status Info **********\n')


def run_dunbrad_spider(location_datas, Q):
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
    for data in location_datas:
        locationID = data[0]
        locationName = data[1]
        url = DNB_BASE + data[2]
        categoryID = data[3]
        process.crawl(LocationPage, start_urls= [url,], q= Q, CategoryID= categoryID, LocationID= locationID, LocationName= locationName)
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
    limite = 50
    max_iter = 10
    it = 1
    if len(sys.argv) > 1:
        i = int(sys.argv[1])
    else:
        i = 3
    # if len(sys.argv) > 2:
    #     limite = int(sys.argv[2])
    # else:
    #     limite = 2
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
            location_datas = SelectItems(DB_NAME, "Location", "CategoryID,", [CategoryID,])
            num_loc_datas = len(location_datas)
            categorys += num_loc_datas
            for loc_idx, location_data in enumerate(location_datas):
                LocationID = location_data[0]
                town_datas = SelectItems(DB_NAME, TB_NAME, "CategoryID, LocationID", [CategoryID, LocationID])
                print (f"{location_data[1]:50s}:\t{(loc_idx+1)*100/num_loc_datas:.2f} %",end= '\r')
                if (len(town_datas) == 0):
                    P = multiprocessing.Process(target= run_dunbrad_spider, args= ([location_data,], Q))
                    P.start()
                    jobs.append(P)
                    time.sleep(0.7)
                num_jobs = len(jobs)
                if num_jobs >= limite:
                    categroy_parse += num_jobs
                    jobs, Q = do_jobs(DB_NAME, TB_NAME, i, category_idx, numCategorys, jobs, Q, num_loc_datas)
            categroy_parse += num_jobs
            jobs, Q = do_jobs(DB_NAME, TB_NAME, i, category_idx, numCategorys, jobs, Q, num_loc_datas)
        total += categorys
        parse += (categorys - categroy_parse)
        print (f"{it:03d} Total:\t{parse * 100 / (total+1e-8):.2f} %")
        if (parse / total+1e-8) == 1 or it >= max_iter:
            break
        it += 1