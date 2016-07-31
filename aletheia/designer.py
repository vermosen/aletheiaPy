'''
Created on Jul 17, 2016

@author: vermosen
'''

class designer:
    '''
    classdocs
    '''
    
    # static members
    tables = []
    buildStatements = []
    
    # ctor
    def __init__(self):
        '''
        Constructor
        '''
        self.tables = ['timeSeries' ,
                       'index'      ,
                       'source'     ,
                       'quandl'     ,
                       'yahoo'      ]
        
        
        self.buildStatements = [
            'CREATE TABLE ' + self.tables[0]
                + '('
                + 'id BIGSERIAL PRIMARY KEY,'
                + 'index INTEGER NOT NULL,'
                + 'date TIMESTAMP NOT NULL,'
                + 'value REAL NOT NULL'
                + ');',
            
            'CREATE TABLE ' + self.tables[1]
                + '('
                + 'id SERIAL PRIMARY KEY,'
                + 'source INTEGER NOT NULL,'
                + 'description VARCHAR(250),'
                + 'startDate TIMESTAMP,'
                + 'endDate TIMESTAMP'
                + ');',
            
            'CREATE TABLE ' + self.tables[2]
                + '('
                + 'id SERIAL PRIMARY KEY,'
                + 'name VARCHAR(250) NOT NULL'
                + ');',
                
            'CREATE TABLE ' + self.tables[3]
                + '('
                + 'id SERIAL PRIMARY KEY,'
                + 'index INT NOT NULL,'
                + 'catalog VARCHAR(50) NOT NULL,'
                + 'key VARCHAR(50) NOT NULL,'
                + 'field VARCHAR(50) NOT NULL'
                + ');',
                
            "CREATE TYPE yahoo_fields AS ENUM ("
                + "'Open', 'High', 'Low', 'Close', "
                + "'Volume', 'Adj Close');",
            
            'CREATE TABLE ' + self.tables[4]
                + '('
                + 'id SERIAL PRIMARY KEY,'
                + 'index INT NOT NULL,'
                + 'key VARCHAR(50) NOT NULL,'
                + 'field yahoo_fields NOT NULL'
                + ');',
            
            'ALTER TABLE ' + self.tables[0]
                + ' ADD CONSTRAINT fk_index_timeSeries'
                + ' FOREIGN KEY (index)'
                + ' REFERENCES index(id)'
                + ' ON DELETE CASCADE;',
            
            'ALTER TABLE ' + self.tables[0]
                + ' ADD CONSTRAINT u_contraint_timeSeries'
                + ' UNIQUE (index, date);',
        
            'ALTER TABLE ' + self.tables[1]
                + ' ADD CONSTRAINT fk_source_index'
                + ' FOREIGN KEY (source)'
                + ' REFERENCES source(id);',
            
            'ALTER TABLE ' + self.tables[3]
                + ' ADD CONSTRAINT fk_index_quandl'
                + ' FOREIGN KEY (index)'
                + ' REFERENCES index(id);',
            
            'ALTER TABLE ' + self.tables[3]
                + ' ADD CONSTRAINT u_contraint_quandl'
                + ' UNIQUE (catalog, key, field);',
                
            'ALTER TABLE ' + self.tables[4]
                + ' ADD CONSTRAINT fk_index_yahoo'
                + ' FOREIGN KEY (index)'
                + ' REFERENCES index(id);',
            
            'ALTER TABLE ' + self.tables[4]
                + ' ADD CONSTRAINT u_contraint_yahoo'
                + ' UNIQUE (key, field);',
            
            'INSERT INTO ' + self.tables[2]
                + ' (name)'
                + " VALUES('quandl');"
                
            'INSERT INTO ' + self.tables[2]
                + ' (name)'
                + " VALUES('yahoo');"

        ]
        
        return