import inject,sys,json
import gzip
import requests
import config
config = config.getConfig()
import threading
from srmse import nutch
from srmse import db
db = db.getMongo(False)

db = db["common-crawl"]
try:
    from cStringIO import StringIO
except:
    from StringIO import StringIO
    
def chunks(l, n):
	"""Yield successive n-sized chunks from l."""
	for i in range(0, len(l), n):
		yield l[i:i+n]
      			
def getDict(url,url_data):
	"""
		dump='/common-crawl/parse-output/segment/{arcSourceSegmentId}/{arcFileDate}_{arcFilePartition}.arc.gz'
	"""
	dic=url_data.replace(url_data.split(" {")[0],"")
	js=eval(dic.replace(" ","").replace("\n",""))
	return js


def fetchDomain(domain):
	print "Fetching domain %s "%(domain)
	col = domain.replace(".","_")
	s = db["fetcher_status"].find_one({"_id":col})
	if s is None:
		urls = db[col].find({"crawled":False}) 
		for url in urls:
			u = url["_id"].replace("#d#",".")
			if threading.active_count() < config["threads_per_domain"]:
				threading.Thread(target = fetchPage, args = (domain, getDict(u,url["url_data"]), u)).start()
			else:
				while True:
					if threading.active_count() < config["threads_per_domain"]:
						threading.Thread(target = fetchPage, args = (domain, getDict(u,url["url_data"]), u)).start()
						break
		db["fetcher_status"].insert_one({"_id":col})
		

def fetchPage(domain,dic,url):
	fetch_url='https://aws-publicdatasets.s3.amazonaws.com/common-crawl/parse-output/segment/%s/%s_%s.arc.gz'%(dic["arcSourceSegmentId"],dic["arcFileDate"],dic["arcFilePartition"])
	resp = requests.get(fetch_url, headers={'Range': 'bytes={}-{}'.format(dic["arcFileOffset"], (dic["arcFileOffset"]+dic["compressedSize"]-1))})
	raw_data = StringIO(resp.content)
	f = gzip.GzipFile(fileobj=raw_data)
	# What we have now is just the WARC response, formatted:
	data = f.read()
	warc=None
	header=None
	response=None
	try:
		header, response = data.strip().split('\r\n\r\n', 2)
	except:
		warc,header, response = data.strip().split('\r\n\r\n', 2)
	if "text/html" in header:
		#only html pages
		dic=nutch.parse(response,url)
		print url
		nutch.insert(dic,"nutch","doc")
		col = domain.replace(".","_")
		key = url.replace(".","#d#")
		db[col].update_one({"_id":key},{"$set":{"crawled":True}})
