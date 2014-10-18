from sqllib import *
import config
import urllib2
import xml.etree.ElementTree as ET
from BeautifulSoup import BeautifulSoup
import hashlib

db = Database(config.db_conf)
db.connectDb()

sitemaps = db.getTableData("sitemaps", ["id", "name", "url"], "flag=1", None, "priority desc")

for row in sitemaps:
    smap = None
    try:
        print "Opening sitemap at " + row['url']
        smap = urllib2.urlopen(row['url'])    
    except :
        print "Failed to open " + row['name'] + " sitemap"
        continue
    
    root = None
    try:
        print "Parsing..."
        root = ET.fromstring(smap.read())   
    except :
        print "Failed to parse " + row['name'] + " sitemap XML"
        continue
    
    nspace = root.tag[:root.tag.find('urlset')]    
    fetched = 0
    skipped = 0
    
    for url in root:
        
        loc = url.find(nspace + "loc")
        
        link = loc.text
        
        if link is None: 
            continue
        else :
            print "Opening url: " + link
        
        if len(db.getTableData("stories", ["id"], "flag=1" 
            + " AND url='" + db.escape(link) + "'")) is 0 :
            
            story = None
            try:
                story = urllib2.urlopen(loc.text) 
            except :
                print "Failed to Open url " + link
                continue
            
            try:
                htm = BeautifulSoup(story.read())
            except :
                print "Failed to parse html from " + link
                continue
            
            permalink = htm.find("meta", property="og:url")['content']
            
            if permalink is None:
                permalink = htm.find("link", rel="canonical")['href']
            
            if permalink is None:
                permalink = url
                
            permalink_hash = abs(hash(permalink)) % (10 ** 13)
            
            title = htm.find("title").text
                    
            if len(db.getTableData("stories", ["id"], "flag=1" 
                + " AND permalink_hash=" + str(permalink_hash)
                + " AND permalink= '" + db.escape(permalink) + "'")) is 0 :
                    
                    db.insertTableData('stories', {'site_id' : row['id'],
                        'permalink_hash' : permalink_hash,
                        'permalink' :  permalink ,
                        'title' :  title ,
                        'url' :  link })
                    
                    db.commitDb()
                        
                    fetched = fetched + 1
                    print "Fetched " + str(fetched) + " from " + row['name']   
            
            else :
                skipped = skipped + 1
                print "Skipped " + str(skipped) + " from " + row['name']   
            
    db.insertTableData('sitemap_fetch', {'site_id' : row['id'], 'fetched' : fetched}) 
    db.commitDb()

        
db.commitDb()
db.closeDb()
            
        
        
            
    
    
