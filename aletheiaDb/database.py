'''
Created on Jul 17, 2016

@author: vermosen
'''

import psycopg2 as db

from aletheiaDb import designer as ds

class database:
    '''
    classdocs
    '''
    
    # members
    designer_ = ds.designer()

    def __init__(self, info):
        self.connection_ = db.connect(database  = info['db'         ],
                                      user      = info['user'       ],
                                      password  = info['password'   ],
                                      host      = info['host'       ],
                                      port      = info['port'       ])
        
    def __del__(self):
        self.connection_.close()
    
    @property
    def connection(self):
        return self.connection_

    # methods
    def dropAll(self):
       
        cur = self.connection_.cursor()
        try:
            for tbl in self.designer_.tables:
                cur.execute('DROP TABLE ' + tbl + ' CASCADE')
            
            self.connection_.commit()
            return
            
        except Exception as e:
            self.connection_.rollback()
            raise ValueError('failed to drop all tables: ' + e + ', closing connection...')
        
    def rebuild(self):

        cur = self.connection_.cursor()
        try:
            for st in self.designer_.buildStatements:
                cur.execute(st)
            
            self.connection_.commit()
            return
            
        except Exception as e: 
            self.connection_.rollback()
            raise ValueError('failed to drop all tables: ' + e + ', closing connection...')
        