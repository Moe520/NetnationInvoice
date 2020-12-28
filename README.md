# Template for using the strategy patten to implement a large scale ETL pipeline for transforming CSV's

This is my go-to design pattern for implementing CSV pipelines that need to have multiple customizable steps.
I encapsulate each in-place operation with a "strategy" and group related strategies together by having an interface for each group.  
Then in process_csv.py I bring in each transform strategy that needs to act on the csv and have them act on it in-place.
Using in-place strategies minimizes the memory used by the csv by avoid unnecessary copying. 
It also allows us to see what the transformed dataframe looks like as it makes its way through the pipeline.


## Dependencies
The following python packages are used for cleanly processing and transforming CSV files:  

pandas  
numpy  


Any modern python interpreter can be used (3.0 or higher)

## Sample outputs

If you want to see the results without running the program (or if the program fails due to unforseen circumstances), there is a copy of the outputs in the "Sample Outputs Folder".


## Instructions
1. Clone the repo onto your machine  ( git clone https://github.com/Moe520/UsageTranslator.git )
2. Put the CSV to be processed at the root of the package (same level as process_csv.py)
3. Run: python process_csv.py --infile [path_to_your_csv_file]  
  example:  
  python process_csv.py --infile Sample_Report.csv  
  
4. Optionally, you can provide your own type_map and reduction_map files by using the optional --typemap and --reductionmap flags

## Assumptions.

The program (for this example ) expects your csv to be in the following format

PartnerID,partnerGuid,accountid,accountGuid,username,domains,itemname,plan,itemType,PartNumber,itemCount
