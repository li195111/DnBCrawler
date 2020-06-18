# ---------Package----------
import scrapy.spiderloader
import scrapy.statscollectors
import scrapy.logformatter
import scrapy.dupefilters
import scrapy.squeues
 
import scrapy.extensions.spiderstate
import scrapy.extensions.corestats
import scrapy.extensions.telnet
import scrapy.extensions.logstats
import scrapy.extensions.memusage
import scrapy.extensions.memdebug
import scrapy.extensions.feedexport
import scrapy.extensions.closespider
import scrapy.extensions.debug
import scrapy.extensions.httpcache
import scrapy.extensions.statsmailer
import scrapy.extensions.throttle
 
import scrapy.core.scheduler
import scrapy.core.engine
import scrapy.core.scraper
import scrapy.core.spidermw
import scrapy.core.downloader
 
import scrapy.downloadermiddlewares.stats
import scrapy.downloadermiddlewares.httpcache
import scrapy.downloadermiddlewares.cookies
import scrapy.downloadermiddlewares.useragent
import scrapy.downloadermiddlewares.httpproxy
import scrapy.downloadermiddlewares.ajaxcrawl
import scrapy.downloadermiddlewares.decompression
import scrapy.downloadermiddlewares.defaultheaders
import scrapy.downloadermiddlewares.downloadtimeout
import scrapy.downloadermiddlewares.httpauth
import scrapy.downloadermiddlewares.httpcompression
import scrapy.downloadermiddlewares.redirect
import scrapy.downloadermiddlewares.retry
import scrapy.downloadermiddlewares.robotstxt
 
import scrapy.spidermiddlewares.depth
import scrapy.spidermiddlewares.httperror
import scrapy.spidermiddlewares.offsite
import scrapy.spidermiddlewares.referer
import scrapy.spidermiddlewares.urllength
 
import scrapy.pipelines
 
import scrapy.core.downloader.handlers.http
import scrapy.core.downloader.contextfactory
# ---------Package----------
import scrapy
import logging
logging.getLogger('scrapy').setLevel(logging.WARNING)
logging.getLogger('scrapy').propagate = False

import settings_v4 as PFBGs
from scrapy.crawler import CrawlerProcess

import multiprocessing
import time
import numpy as np

class PFBGSpider(scrapy.Spider):
	name = "PFBGSpider"
	start_urls = ['http://vip.win007.com/AsianOdds_n.aspx?id=1667534']
	#start_urls = ['http://vip.win007.com/changeDetail/handicap.aspx?id=1509841&companyID=3&l=0']
	q = None
	def parse(self, response):
		try:
			#print (response.status, response.url)
			#if response.status != 200:
			#	print ("Error :", response.url, "\n", response)
			if response.url == 'http://vip.win007.com/customErrPage.htm?aspxerrorpath=/OverDown_n.aspx':
				self.error(response)

			tables = response.xpath("//div//table")
			trs = tables[1].xpath("tr")
			crown = []
			for i in range(len(trs)):
				trs_tds = trs[i].xpath("td").xpath("text()").extract()[0][:5]
				if trs_tds == "Crown":
					tds_init = trs[i].xpath("td[@title]").xpath("text()").extract()
					tds_end = trs[i].xpath("td[@oddstype='wholeOdds']").xpath("text()").extract()
					#crown.append(trs_tds)
					if len(tds_init) > 0:
						for j in range(len(tds_init)):
							crown.append(tds_init[j])
					else:
						for j in range(3):
							crown.append("")
					if len(tds_end) > 0:
						for j in range(len(tds_end)):
							crown.append(tds_end[j])
					else:
						for j in range(3):
							crown.append("")
			self.q.put(crown)
			return
		except Exception as e:
			self.error(response)
			print (e)

	def error(self, response):
		print ("Error :", self.start_urls)
		crown = []
		crown.append("Page")
		crown.append("Error")
		crown.append(response.url)
		crown.append(self.start_urls)
		crown.append("")
		crown.append("")
		self.q.put(crown)
		return



class PFBGSpiderDetail(scrapy.Spider):
	name = "PFBGSpiderDetail"
	start_urls = ['http://vip.win007.com/changeDetail/handicap.aspx?id=1509841&companyID=3&l=0']
	q = None
	def parse(self, response):
		try:
			#print (response.status, response.url)
			tables = response.xpath("//div[@id='out']").xpath("//table[@cellspacing= '1']")
			crown = []
			trs = tables.xpath("tr[@bgcolor= '#fff4f4']")
			tds = []
			t_60 = []
			t_70 = []
			t_mid = []

			for i in range(len(trs)):
				td = trs[i].xpath("td")
				t = td[0].xpath("text()").extract()[0]
				if t == '中场':
					td_len = len(td.extract())
					if td_len != 5:
						td1 = td[0].xpath("text()").extract()[0]
						td2 = td[1].xpath("text()").extract()[0]
						td3 = td[2].xpath("font//b/text()").extract()[0]
						td4 = td[3].xpath("font/text()").extract()[0]
						td5 = td[4].xpath("font//b/text()").extract()[0]
						td6 = td[5].xpath("text()").extract()[0]
						td7 = td[6].xpath("text()").extract()[0]
						d = [td1, td2, td3, td4, td5, td6, td7]
						t_mid.append(d)

				if t != '中场' and int(float(t)) > 0:
					td_len = len(td.extract())
					if td_len != 5:
						td1 = td[0].xpath("text()").extract()[0]
						td2 = td[1].xpath("text()").extract()[0]
						td3 = td[2].xpath("font//b/text()").extract()[0]
						td4 = td[3].xpath("font/text()").extract()[0]
						td5 = td[4].xpath("font//b/text()").extract()[0]
						td6 = td[5].xpath("text()").extract()[0]
						td7 = td[6].xpath("text()").extract()[0]
						d = [td1, td2, td3, td4, td5, td6, td7]
						tds.append(d)

			mins = np.array([int(float(item[0])) for item in tds])
			if len(mins) != 0:
				min_mins_mid = np.min(np.abs(mins - 45))
				min_mins_60 = np.min(np.abs(mins - 60))
				min_mins_70 = np.min(np.abs(mins - 70))
				t_60 = [item for item in tds if abs(int(float(item[0])) - 60) == min_mins_60]
				t_70 = [item for item in tds if abs(int(float(item[0])) - 70) == min_mins_70]
				if len(t_mid) > 0:
					t_mid = t_mid[0]
				else:
					t_mid = [item for item in tds if abs(int(float(item[0])) - 45) == min_mins_mid]
					if len(t_mid) > 0:
						t_mid = t_mid[0]

				if len(t_60) > 0:
					t_60 = t_60[0]

				if len(t_70) > 0:
					t_70 = t_70[0]
			else:
				pass
			if t_mid != t_60 and t_mid != t_70:
				crown.append(t_70)
				crown.append(t_60)
				crown.append(t_mid)
			else:
				t_60 = []
				t_70 = []
				crown.append(t_70)
				crown.append(t_60)
				crown.append(t_mid)
			self.q.put(crown)
		except Exception as e:
			self.error(response)
			print (e)
		return

	def error(self, response):
		print ("\nError :", self.start_urls, '\n')
		crown = []

		t_70 = ["Page Error"]
		t_70.extend([''] * 6)

		t_60 = [response.url]
		t_60.extend([''] * 6)

		t_mid = self.start_urls
		t_mid.extend([''] * 6)

		crown.append(t_70)
		crown.append(t_60)
		crown.append(t_mid)
		self.q.put(crown)
		return

def run_spider(url, q):
	process = CrawlerProcess({'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'})
	process.crawl(PFBGSpider, start_urls = [url,], q = q)
	process.start()

def run_spider_detail(url, q):
	process = CrawlerProcess({'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'})
	process.crawl(PFBGSpiderDetail, start_urls = [url,], q = q)
	process.start()

if __name__ == "__main__":
	id = '1650940'
	game_web = 'http://vip.win007.com/AsianOdds_n.aspx?id=' + id
	q_handicap = multiprocessing.Queue()
	P_handicap = multiprocessing.Process(target = run_spider, args= (game_web, q_handicap,))
	game_detal_web = 'http://vip.win007.com/changeDetail/handicap.aspx?id=' + id + '&companyID=3&l=0'
	q_detal_handicap = multiprocessing.Queue()
	P_detal_handicap = multiprocessing.Process(target = run_spider_detail, args= (game_detal_web, q_detal_handicap,))
	game_numb_web = 'http://vip.win007.com/OverDown_n.aspx?id=' + id + '&l=0'
	q_overunder = multiprocessing.Queue()
	P_overunder = multiprocessing.Process(target = run_spider, args= (game_numb_web, q_overunder,))
	game_numb_detail_web = 'http://vip.win007.com/changeDetail/overunder.aspx?id=' + id + '&companyID=3&l=0'
	q_detal_overunder = multiprocessing.Queue()
	P_detal_overunder = multiprocessing.Process(target = run_spider_detail, args= (game_numb_detail_web, q_detal_overunder,))
	try:
		P_handicap.start()
		P_detal_handicap.start()
		P_overunder.start()
		P_detal_overunder.start()

		handicap = q_handicap.get()
		detal_handicap = q_detal_handicap.get()
		overunder = q_overunder.get()
		detal_overunder = q_detal_overunder.get()

		P_handicap.join()
		P_detal_handicap.join()
		P_overunder.join()
		P_detal_overunder.join()
		pass
	except Exception as e:
		print ("Error :", e)
		print ("Retry")

	#print (handicap)
	#game_company = handicap[0]
	#game_init_handicaphome = handicap[1]
	#game_init_handicap = handicap[2]
	#game_init_handicapaway = handicap[3]
	#game_final_handicaphome = handicap[4]
	#game_final_handicap = handicap[5]
	#game_final_handicapaway = handicap[6]
	#print (game_init_handicaphome, game_init_handicap, game_init_handicapaway)
	#print (game_final_handicaphome, game_final_handicap, game_final_handicapaway)

	print (detal_handicap)

	#print (overunder)
	#game_init_overunderhome = overunder[1]
	#game_init_overunder = overunder[2]
	#game_init_overunderaway = overunder[3]
	#game_final_overunderhome = overunder[4]
	#game_final_overunder = overunder[5]
	#game_final_overunderaway = overunder[6]
	#print (game_init_overunderhome, game_init_overunder, game_init_overunderaway)
	#print (game_final_overunderhome, game_final_overunder, game_final_overunderaway)

	print (detal_overunder)
	pass
