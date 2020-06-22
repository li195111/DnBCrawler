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
                'DOWNLOAD_DELAY': 1,
                'CONCURRENT_REQUESTS': 16,
                'TELNETCONSOLE_PORT' : None,
                'TELNETCONSOLE_ENABLED':False
            }
        )
    for data in location_datas:
        categoryID = data[0]
        locationID = data[1][0]
        locationName = data[1][1]
        url = DNB_BASE + data[1][2]
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
    # for i in range(num_industry):
    if len(sys.argv) > 1:
        i = int(sys.argv[1])
    else:
        i = 0
    if len(sys.argv) > 2:
        limite = int(sys.argv[2])
    else:
        limite = 2
    while True:
        IndustryID = GetItemID(DB_NAME, "Industry", [i+1])
        category_datas = SelectItems(DB_NAME, "Category", "IndustryID,", [IndustryID,])
        numCategorys = len(category_datas)
        NewTownDatas = []
        location_name = ""
        for idx, category_data in enumerate(category_datas):
            CategoryID = category_data[0]
            if len(NewTownDatas) >= limite:
                break
            location_datas = SelectItems(DB_NAME, "Location", "CategoryID,", [CategoryID,])
            num_loc_datas = len(location_datas)
            for loc_idx, location_data in enumerate(location_datas):
                LocationID = location_data[0]
                if len(NewTownDatas) >= limite:
                    break
                town_datas = SelectItems(DB_NAME, "Town", "CategoryID, LocationID", [CategoryID, LocationID])
                if (len(town_datas) == 0):
                    location_name = location_data[1]
                    NewTownDatas.append([CategoryID, location_data])
        num_add = len(NewTownDatas)
        print (f"Industry {i+1:02d} {location_name} ... Number to add:\t{num_add}")
        Q = multiprocessing.Queue()
        jobs = []
        for data in NewTownDatas:
            P = multiprocessing.Process(target= run_dunbrad_spider, args= ([data,], Q))
            P.start()
            jobs.append(P)
        for P in jobs:
            P.join(timeout= num_add * 2 if num_add > 10 else 20)
        TownData = []
        res = -1
        while not Q.empty():
            loc_data = Q.get(timeout= 5)
            res = loc_data["result"]
            locationID = loc_data["location"]
            locationName = loc_data["location_name"]
            categoryID = loc_data["category"]
            town = loc_data["data"]
            if res == 0:
                if len(town) > 0:
                    for loc_name in town:
                        url = town[loc_name]
                        TownData.append((loc_name, url, categoryID, locationID))
            elif res == 1:
                url = town
                TownData.append((locationName, url, categoryID, locationID))
        if len(TownData) == 0:
            limite += 1
        print (f"Industry {i+1:02d} Town ... Num insert\t{len(TownData)}")
        INSERT(DB_NAME, "Town", TownData)
        if num_add == 0 and res == 0:
            break