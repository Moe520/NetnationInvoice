from abc import ABCMeta, abstractmethod


class DropRowsStrategy(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def drop_bad_rows(self, data_ref):
        """ Drop rows from the dataframe using the provided strategy"""
