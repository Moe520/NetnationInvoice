from abc import ABCMeta, abstractmethod


class DropRowsStrategy(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def drop_bad_rows(self, data_ref, error_logger):
        """ Drop rows from the data frame using the provided strategy"""
1