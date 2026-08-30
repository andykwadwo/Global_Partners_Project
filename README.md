"# Global_Partners_Project" 
This project builds a pipeline using Medallion architecture and AWS services only.
Data is ingested into Microsoft SQL Server using SSMS. AWS Glue then used to do
load the raw data into an S3 Bucket. AWS Glue is then used to used to transform data
into silver and gold layers.

Customer Lifetime Value (CLV) Metrics:
can be found in the gp_CLV_gold_table

Customer Segmentation & Behavior Metrics:
can be found in the below folders
