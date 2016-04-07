from multiprocessing import Pool
import os
li=open("domains.txt","r").read().split("\n")
def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i+n]

sublists=list(chunks(li,5))

def Crawler(domain):
	os.system("sudo python test.py %s"%(domain))
	
for sub in sublists:
	print sub
	p = Pool(5)
	p.map(Crawler,sub)
	p.join()
