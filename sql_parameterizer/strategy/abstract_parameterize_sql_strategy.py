from abc import ABCMeta, abstractmethod


class ParameterizeSqlStrategy:
    __metaclass__ = ABCMeta

    @abstractmethod
    def parameterize_df_to_sql(self, data_ref):
        """ Generate a column of sql inserts """
