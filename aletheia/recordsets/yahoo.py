'''
Created on Jul 19, 2016

@author: vermosen
'''

from aletheia.recordset import recordset

from psycopg2.extras import DictCursor

class yahooRecordset(recordset):
    
    def __init__(self, db):
        self.conn_ = db.connection
        cur = self.conn_.cursor()
        cur.execute("SELECT id FROM source WHERE name = 'yahoo'")
        
        # quandl source id
        self.id_ = cur.fetchone()[0];

    # rec format: key | field | description
    def insert(self, rec):
        
        try:
            cur = self.conn_.cursor()
            cur.execute("INSERT INTO index (source, description) VALUES (" + 
                        str(self.id_) + ", '" + str(rec[2]) + "') RETURNING id")
            new_id = cur.fetchone()[0];
            cur.execute("INSERT INTO yahoo (index, key, field) VALUES (" + 
                        str(new_id) + ", '" + 
                        str(rec[0]) + "', '" + 
                        str(rec[1]) + "') RETURNING id")
            
            self.conn_.commit()
            
        except Exception as e:
            self.conn_.rollback()
            raise e
             
        
    def select(self, *args):
        
        if len(args) == 1 and isinstance(args[0], str):
            return self.__selectWithStatement(args[0])
        
        elif len(args) == 2 and isinstance(args[0], str) \
            and isinstance(args[1], str):
            return self.__selectWithKeys(args[0], args[1])
        else:
            raise TypeError("wrong types passed in function select")
                
    def __selectWithStatement(self, statement): 
         
        cur = self.conn_.cursor(cursor_factory=DictCursor)
        
        st  = "SELECT index.id as id, yahoo.key as key, "
        st += "yahoo.field as field, "
        st += "index.description as description "
        st += "FROM index INNER JOIN yahoo ON (index.id = yahoo.index)"
        
        if (statement != "*"):
            st = st + " WHERE (" + statement + ")"
        
        cur.execute(st)
        
        return cur.fetchall()
    
    def __selectWithKeys(self, key, field):  
        
        cur = self.conn_.cursor(cursor_factory=DictCursor)
        
        st  = "SELECT index.id as id, yahoo.key as key, "
        st += "yahoo.field as field, "
        st += "index.description as description "
        st += "FROM index INNER JOIN yahoo ON (index.id = yahoo.index) "
        st += "WHERE (key = '" + key + "' AND "
        st += "field = '" + field + "')"
        
        cur.execute(st)
        
        return cur.fetchall()
    
    def update(self, rec): pass
    def delete(self, rec): pass