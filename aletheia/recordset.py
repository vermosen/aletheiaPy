'''
Created on Jul 17, 2016

@author: vermosen
'''
from abc import ABCMeta, abstractmethod

class recordset:
    '''
    classdocs
    '''
    __metaclass__ = ABCMeta

    @abstractmethod
    def insert(self, rec):
        raise NotImplementedError()
    
    @abstractmethod
    def select(self, statement):
        raise NotImplementedError()
    
    @abstractmethod
    def update(self, rec):
        raise NotImplementedError()
    
    @abstractmethod
    def delete(self, rec):
        raise NotImplementedError()