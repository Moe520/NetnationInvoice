#Program for transforming CSV data and generating SQL insert statements


## Dependencies
The following python packages are used for cleanly processing and transforming CSV files:
pandas
numpy

Any modern python interpreter can be used (3.0 or higher)

## Instructions
1. Clone the repo onto your machine  ( git clone https://github.com/Moe520/UsageTranslator.git )
2. Put the CSV to be processed at the root of the package (same level as process_csv.py)
3. Run: python process_csv.py --infile [path_to_your_csv_file]
4. Optionally, you can provide your own type_map and reduction_map files by using the optional --typemap and --reductionmap
