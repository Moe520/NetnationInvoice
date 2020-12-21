class SkippableRowDropper:
    def __init__(self, drop_rows_strategy):
        self.dropping_strategy = drop_rows_strategy

    def drop_bad_rows(self, data_ref, error_logger):
        self.dropping_strategy.drop_bad_rows(self.dropping_strategy, data_ref, error_logger)
