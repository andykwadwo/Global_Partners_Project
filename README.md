"# Global_Partners_Project" 
This project builds a pipeline using Medallion architecture and AWS services only.
Data is ingested into Microsoft SQL Server using SSMS. AWS Glue then used to do
load the raw data into an S3 Bucket. AWS Glue is then used to used to transform data
into silver and gold layers.

Customer Lifetime Value (CLV) Metrics:
can be found in the gp_CLV_gold_table

Customer Segmentation & Behavior Metrics:
can be found in the below folders
The distinct segment that emerged when customers are grouped by recency days, frequency count and monetary value
was the recency days. This is due to the freshness of the data being worked with.

Churn Risk Indicators Dashboard:
Script used for generating this metric can be found in the folder gb_CHURNIND_gold_table.


Sales Trends and Seasonality Dashboard:
Scripts used for this metric can be found in gp_daily_rev_gold_table, gp_weekly_rev_gold_table and gp_monthly_rev_gold_table
folders. Visualization shows the totals for weekly and monthly revenues.
