#Program for transforming CSV data and generating SQL insert statements


## Dependencies
The following python packages are used for cleanly processing and transforming CSV files:  

pandas  
numpy  


Any modern python interpreter can be used (3.0 or higher)

## Sample outputs

If you want to see the results instead of running the program (or if the program fails due to unforseen circumstances), there is a copy of the outputs in the "Sample Outputs Folder".


## Instructions
1. Clone the repo onto your machine  ( git clone https://github.com/Moe520/UsageTranslator.git )
2. Put the CSV to be processed at the root of the package (same level as process_csv.py)
3. Run: python process_csv.py --infile [path_to_your_csv_file]  
  example:  
  python process_csv.py --infile Sample_Report.csv  
  
4. Optionally, you can provide your own type_map and reduction_map files by using the optional --typemap and --reductionmap flags

## Assumptions.

The program expects your csv to be in the following format

PartnerID,partnerGuid,accountid,accountGuid,username,domains,itemname,plan,itemType,PartNumber,itemCount
