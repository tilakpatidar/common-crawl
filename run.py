#Main file of the crawler

"""
	To make config changes go to config.py
	Author Tilak Patidar

"""

import inject
import parse
import config
from multiprocessing import Pool
import fetcher

config = config.getConfig()

domains = open("domains.txt","r").read().split("\n")
domains.pop() #empty element pop


print "Domain list read"

for domain in domains:
	#creating seed urls
	#stores urls in temp files in url_lists/
	inject.prepareDomain(domain)

#now parse each domain file
#insert in db urls
for domain in domains:
	parse.createUrlList(domain)
	


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i+n]
        
#create sublists of domains and then fetch


sublists = list(chunks(domains,config["concurrent_domains"]))

for sub in sublists:
	print sub
	p = Pool(config["concurrent_domains"])
	p.map(fetcher.fetchDomain,sub)
	p.join()
	
