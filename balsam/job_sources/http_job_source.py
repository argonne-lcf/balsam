import urllib2

def get_jobs():    
    job_source_url = 'http://www.mcs.anl.gov/~turam/balsam/jobs'
    response = urllib2.urlopen(job_source_url)
    html = response.read()
    urls = html.split('\n')
    urls = [u for u in urls if len(u) > 0]
    return urls

if __name__ == '__main__':
    print get_job_urls()
