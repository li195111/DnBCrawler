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

DNB_BASE = 'https://www.dnb.com'
class CategoryPage(scrapy.Spider):

    name = "category_spider"
    base_url = 'https://www.dnb.com'
    start_urls = []
    q = None
    reg = ""
    loc = ""
    def parse(self, response):
        # print('\n********** Second Status Info **********\n')
        if response.status == 200:
            print(f"URL :\t\t{response.url} ... OK !")
            load_region_info = response.css("h1.title::text")
            load_error = 'Oh no! 500 Error' in load_region_info.extract() or 'Oh no! 404 Error' in load_region_info.extract()
            next_page = 0
            location_towns = {}
            if not load_error:
                towns = response.xpath("//div[@id='locationResults']").css("div.data")
                if len(towns.extract()) > 0:
                    for town in towns:
                        name = town.css("a::text").extract_first().strip()
                        url = town.css("a::attr('href')").extract_first()
                        location_towns[name] = url
                else:
                    next_page = 1
                    location_towns = response.url.replace(self.base_url,"")
            # print (f"Number of Region Locations: {len(region_locations)}")
            out = {"result":next_page,"data":location_towns,"reg":self.reg,"loc":self.loc}
            # print (f"Load Error: {load_error}, {next_page}, {len(location_towns)}")
            self.q.put(out)
        else:
            print(f"URL :\t\t{response.url} ... Error Code: {response.status}\n")
        # print('\n********** Second Status Info **********\n')


def run_dunbrad_spider(region_datas, Q):
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
    for data_idx in range(len(region_datas)):
        reg = region_datas[data_idx]["region"]
        loc = region_datas[data_idx]["location"]
        url = DNB_BASE + region_datas[data_idx]["url"]
        process.crawl(CategoryPage, start_urls= [url,], q= Q, reg= reg, loc= loc)
    process.start()
    # process.stop()
    
def Hello(url, q):
    print (f"Hello ... {url}")
    return 

if __name__ == "__main__":
    CUR_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    INDUSTRY_DIR = os.path.join(CUR_PATH,"Industrys")
    ALL_INDUSTRY = os.listdir(INDUSTRY_DIR)
    
    num_industry = len(ALL_INDUSTRY)
    # for i in range(num_industry):
    limite = 10
    if len(sys.argv) > 1:
        i = int(sys.argv[1])
    else:
        i = 2
    file_name = f"Industry_{i}.json"
    file_path = os.path.join(INDUSTRY_DIR, file_name)
    while True:
        num_log_items = False
        indusrty_category_regions_locations = []
        with open(file_path, 'r', encoding= 'utf-8') as f:
            industry_datas = json.load(f)
            num_category = len(industry_datas)
            for idx, data in enumerate(industry_datas):
                category = data["Category"]
                if ("Locations" in data) and (len(data["Locations"]) != 0):
                    RegionDatas = data["Locations"]
                    if not ("Towns" in data):
                        data["Towns"] = {}
                    NewRegionDatas = []
                    for reg in RegionDatas:
                        if len(NewRegionDatas) >= limite:
                            break
                        for loc in RegionDatas[reg]:
                            if len(NewRegionDatas) >= limite:
                                break
                            url = RegionDatas[reg][loc]
                            if not reg in data["Towns"]:
                                NewRegionDatas.append({"region":reg,"location":loc,"url":url})
                            else:
                                if not loc in data["Towns"][reg]:
                                    NewRegionDatas.append({"region":reg,"location":loc,"url":url})
                    num_add = len(NewRegionDatas)
                    print (f"{category}\t... Number to add:\t{num_add}")
                    Q = multiprocessing.Queue()
                    P = multiprocessing.Process(target= run_dunbrad_spider, args= (NewRegionDatas, Q))
                    P.start()
                    P.join(timeout= num_add * 1.5 if num_add > 20 else 20)
                    log_items = [num_log_items]
                    while not Q.empty():
                        town_data = Q.get(timeout= 5)
                        res = town_data["result"]
                        towns = town_data["data"]
                        reg = town_data["reg"]
                        loc = town_data["loc"]
                        if not reg in data["Towns"]:
                            data["Towns"][reg] = {}
                        if res == 0:
                            if len(towns) > 0:
                                data["Towns"][reg][loc] = towns
                                log_items.append(True)
                            print (reg, loc, len(towns))
                        else:
                            data["Towns"][reg][loc] = {loc:towns}
                            log_items.append(True)
                            print (reg, loc, 1)
                    num_log_items = (np.array(log_items) == True).any()
                indusrty_category_regions_locations.append(data)
        if num_log_items:
            with open(file_path, 'w', encoding= 'utf-8') as f:
                json.dump(indusrty_category_regions_locations, f)
        else:
            break
