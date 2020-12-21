from abc import ABCMeta, abstractmethod


class ColumnPrepStrategy(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def prep_column(self, data_ref,type_map,reduction_map):
        """ Apply the appropriate transform to the column """
