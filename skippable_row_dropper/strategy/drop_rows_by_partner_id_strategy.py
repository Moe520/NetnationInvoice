from skippable_row_dropper.strategy.abstract_drop_rows_strategy import DropRowsStrategy


class DropRowsByPartnerIdStrategy(DropRowsStrategy):
    """
    Drops rows with blacklisted partner ID

    For now the list is hard coded since its tine. If it gets bigger can keep in text file and
    import as a set for fast lookup

    """
    BAD_PARTNER_ID_LIST = [26392]

    def drop_bad_rows(self, data_ref, error_logger):
        """
        :param data_ref: dataframe to drop rows from
        :param error_logger: logger to use when reporting bad rows
        :return:
        """
        error_logger.log_to_file_single("Dropped Rows based on following Partner Id blacklist: " +
                                        str(self.BAD_PARTNER_ID_LIST) + ' \n' + '\r' + '\r\n')

        target_list = data_ref[data_ref["PartnerID"].isin(self.BAD_PARTNER_ID_LIST)].index

        data_ref.drop(target_list, inplace=True)
