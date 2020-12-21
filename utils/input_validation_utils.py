import argparse

def get_terminal_args(type_map_path,reduction_map_path):
    """
    Grabs the command line arguments
    :param type_map_path: optional path to your own type maps.
    :param reduction_map_path: optional path to your own reduction map
    :return:
    """
    # Fetch the command line arguments
    parser = argparse.ArgumentParser(description="""
     This script will generate sql insert statements based on a given invoice csv 
    """)

    parser.add_argument("--infile", help="Path to the csv file to be processed",
                        required=True
                        )

    parser.add_argument("--typemap",
                        help="Optional Path to typemap.json file",
                        required=False,
                        default=type_map_path
                        )

    parser.add_argument("--reductionmap",
                        help="Optional Path to typemap.json file",
                        required=False,
                        default=reduction_map_path
                        )

    args = parser.parse_args()

    return args

