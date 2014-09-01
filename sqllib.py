import MySQLdb
class Database:
    def __init__(self, params):
        self.host = params['host']
        self.user = params['user']
        self.password = params['password']
        self.dbname = params['dbname']
        self.conn = None
        
    def connectDb(self):
        if self.conn is None:
            db = MySQLdb.connect(host=self.host, user=self.user, passwd=self.password, db=self.dbname)
            self.conn = db.cursor()
    
    def executeSql(self, query):
        self.conn.execute(query)
        return self.conn.fetchall()    
    
    def getTableData(self, table, column="*", condition=None, group=None, order=None, limit=None, debug=False):
        query = "SELECT "
        if column is not None and column is not "*":
            query += ",".join(column)
        else:
            query += "*"
            
        query += " FROM " + table
        
        if condition is not None:
            query += " WHERE "+ condition
            
        if group is not None:
            query += " GROUP BY " + group
            
        if order is not None:
            query += " ORDER BY " + order
            
        if limit is not None:
            query += " LIMIT " + limit
            
        if debug == True:
            print query
        
        result = self.executeSql(query)
        
        cols = [i[0] for i in self.conn.description]
        
        data = [{cols[i]:column for i, column in enumerate(value)} for value in result]
        
        return data
