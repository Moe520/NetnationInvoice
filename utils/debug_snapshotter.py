def show_snapshot_if_debugging(data_sample,debug_flag):
    if debug_flag:
        print(data_sample.head())