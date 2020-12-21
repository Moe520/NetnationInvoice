class SqlParameterizer:
    def __init__(self, parameterize_sql_strategy):
        self.parameterize_sql_strategy = parameterize_sql_strategy

    def parameterize_df_to_sql(self, data_ref):
        self.parameterize_sql_strategy.parameterize_df_to_sql(self.parameterize_sql_strategy, data_ref)