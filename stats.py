from sqllib import *
import config
import urllib2
import xml.etree.ElementTree as ET
from BeautifulSoup import BeautifulSoup
import hashlib
import json

db = Database(config.db_conf)
db.connectDb()

stories = db.getTableData("stories as s left join story_fetch as sf on s.id=sf.story_id",
        ["s.id", "s.permalink", "s.permalink_hash"], 
        "(sf.locked=0 or sf.locked is null) and s.flag=1", 
        None, 
        "sf.update_count asc, sf.last_update asc, s.id desc", 
        str(config.batch_size))

if len(stories)>0 :
    
    lockQuery = "INSERT INTO story_fetch (story_id, locked) VALUES"
    
    unlockQuery = "UPDATE story_fetch SET locked=0 WHERE story_id in ("

    i=0
    for row in stories:
        lockQuery += "(" + str(row['id']) + "," + "1)"
        unlockQuery += str(row['id'])
        i=i+1
        if i<len(stories):
            lockQuery += ","
            unlockQuery += ","
    
    lockQuery += "ON DUPLICATE KEY UPDATE locked = VALUES(locked)"    
    unlockQuery += ") AND locked!=0"

    db.executeSql(lockQuery)
    
    db.commitDb()
    
    urlBatch = config.fb_graph_url
    urlId = {}
    
    i=0
    
    for row in stories:
        if len(urlBatch+row['permalink'])>=1700 or i==len(stories)-1 :
            urlBatch += row['permalink'] 
            urlId[row['id']] = row['permalink']
            
            #try:                
            result = urllib2.urlopen(urlBatch) 
            rdata = json.load(result) 
                            
            for key, value in urlId.iteritems():
                share = 0
                if value in rdata:
                    rs = rdata[value]
                    if 'shares' in rs:
                        share += int(rs['shares'])
                    if 'comments' in rs:
                        share += int(rs['comments'])
                    print str(key) + ":" + str(share)
                    
                    db.insertTableData('story_stats', {'story_id' : key, 'count' : share}) 
                    db.updateTableData('story_fetch', 'update_count=update_count+1, locked=0', 'story_id='+str(key))
                    db.commitDb()
                    
            #except :
             #   print "Batch failed"
                
            urlBatch = config.fb_graph_url
            urlId = {} 
        else:
            urlBatch += row['permalink'] + ","
            urlId[row['id']] = row['permalink']
        i=i+1
        
    
    db.executeSql(unlockQuery)
    
    db.commitDb()

    
