'''
Created on Jul 19, 2016

@author: vermosen
'''

from aletheia.recordset import recordset

from psycopg2.extras import DictCursor

class timeseriesRecordset(recordset):
    
    def __init__(self, db):
        self.conn_ = db.connection

    # rec format: tuple with id | timespan | value
    def insert(self, recs):
        
        cur = self.conn_.cursor()
        
        for rec in recs:
                        
            try:
                # TODO: depending on the database policy
                statement = "INSERT INTO timeSeries (index, date, value) VALUES (" + \
                    str(rec[0]) + ", '" + str(rec[1]) + "', '" + str(rec[2]) + "') " + \
                    "ON CONFLICT DO NOTHING" 
                
                cur.execute(statement)
                self.conn_.commit()

            except Exception as e:
                print('warning:' + str(e))
                self.conn_.rollback()
                
                # TODO: try to update the record instead

    def select(self, statement):  
        cur = self.conn_.cursor(cursor_factory=DictCursor)
        st = "SELECT quandl.key, quandl.catalog, quandl.field, index.description "
        st += "FROM index INNER JOIN quandl ON (index.id = quandl.index)"
        
        if (statement != None and statement != "*"):
            st = st + " AND (" + statement + ")"
        
        cur.execute(st)
        
        return cur.fetchall()
    
    def getLastDate(self, idx):
        cur = self.conn_.cursor(cursor_factory=DictCursor)
        
        st = "SELECT MAX(date) as max_date FROM timeSeries "
        st += "WHERE index = " + str(idx)
    
        cur.execute(st)
        return cur.fetchall()
        
    def update(self, rec): pass
    def delete(self, rec): pass