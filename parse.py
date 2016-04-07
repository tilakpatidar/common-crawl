
from srmse import db
import time
db = db.getMongo()

db = db["common-crawl"]
	
	
def createUrl(a):
	"""
		Converted nutch style url to normal url
	
	"""
	o=a
	a=a.replace("/:https","/:http")
	a="/".join(a.split("/")[:-1])
	in_d=a.split("/")[0]
	d=in_d.split(".")[::-1]
	d=".".join(d)
	url=d+"/".join(o.replace(in_d,"").replace("/:http","").split("/"))
	return "http://"+(url.replace(":http","").replace("//","/").strip())


def searchKey(a):
	"""Escapes . for mongo keys """
	return a.replace(".","#d#")

def createUrlList(domain):
	li=open("url_lists/%s"%(domain.replace(".","#")),"r").read().split("\n")
	col = domain.replace(".","_")
	f = db["stats"].find_one({"_id":col})
	if f is None:
		for ll in li:
			k=createUrl(ll.split(" {")[0].strip())
		
			search = searchKey(k)
		
			if not db[col].find_one({"_id":search}):
				#url does not exists begin
				print "Discovered %s" %(k)
				db[col].insert({"_id":search,"crawled":False,"url_data":ll,"update_time":int(time.time())},w=0)
		db["stats"].insert_one({"_id":col}) #mark parsing done
	else:
		print "parsing already done %s"%(col)
