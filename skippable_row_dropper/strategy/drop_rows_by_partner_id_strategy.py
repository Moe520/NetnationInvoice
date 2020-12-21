from skippable_row_dropper.strategy.abstract_drop_rows_strategy import DropRowsStrategy


class DropRowsByPartnerIdStrategy(DropRowsStrategy):
    BAD_PARTNER_ID_LIST = [26392]

    def drop_bad_rows(self, data_ref, error_logger):
        error_logger.log_to_file_single("Dropped Rows based on following Partner Id blacklist: " +
                                        str(self.BAD_PARTNER_ID_LIST) + ' \n' + '\r' + '\r\n')

        target_list = data_ref[data_ref["PartnerID"].isin(self.BAD_PARTNER_ID_LIST)].index

        data_ref.drop(target_list, inplace=True)
