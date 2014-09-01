from sqllib import *
import config
import urllib2
import xml.etree.ElementTree as ET
from BeautifulSoup import BeautifulSoup

db = Database(config.db_conf)
db.connectDb()

sitemaps = db.getTableData("sitemaps", ["id", "name", "url"], "flag=1", None, "priority desc")

for row in sitemaps:
    smap = urllib2.urlopen(row['url'])    
    root = ET.fromstring(smap.read())    
    nspace = root.tag[:root.tag.find('urlset')]    
    for url in root:
        loc = url.find(nspace + "loc")
        story = urllib2.urlopen(loc.text) 
        htm = BeautifulSoup(story.read())
        
        permalink = htm.find("meta", property="og:url")['content']
        
        if permalink is None:
            permalink = htm.find("link", rel="canonical")['href']
        
        if permalink is None:
            permalink = url
        
            
    
    
