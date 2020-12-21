def show_snapshot_if_debugging(msg,data_sample,debug_flag):
    """
    :param msg: Message to display above the snapshot
    :param data_sample: small sample of the data currently in memory
    :param debug_flag: if true will print snapshot to screen
    :return:
    """
    if debug_flag:
        print(msg)
        print(data_sample.head())