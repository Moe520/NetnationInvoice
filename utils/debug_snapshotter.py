def show_snapshot_if_debugging(msg,data_sample,debug_flag):
    if debug_flag:
        print(msg)
        print(data_sample.head())