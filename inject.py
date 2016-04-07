import os
def prepareDomain(domain):
	d = domain.replace(".","#")
	if not d in os.listdir("./url_lists"):
		os.system("bin/index_lookup_remote %s > %s"%(domain,domain.replace(".","#")))
		print "Got the fetch list for %s"%(domain)
		os.system("mv %s url_lists/%s"%(domain.replace(".","#"),domain.replace(".","#")))
		print "File moved %s"%(domain.replace(".","#"))

