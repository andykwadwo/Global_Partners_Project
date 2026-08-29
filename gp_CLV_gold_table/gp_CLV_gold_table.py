import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1787761503951 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://global-partners-oseikwadwo/silver/order_items/run-1787708185380-part-r-00000"], "recurse": True}, transformation_ctx="AmazonS3_node1787761503951")

# Script generated for node Amazon S3
AmazonS3_node1787761479156 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://global-partners-oseikwadwo/bronze/order_item_options/run-1787688064766-part-r-00000"], "recurse": True}, transformation_ctx="AmazonS3_node1787761479156")

# Script generated for node SQL Query
SqlQuery484 = '''
WITH TotalRevenue AS (
select
    oio.ORDER_ID,
    oi.USER_ID,
    SUM(oi.ITEM_PRICE * oi.ITEM_QUANTITY) AS rev_per_order
FROM  
    myDataSourceoio oio
join
    myDataSourceoi oi
on
    oi.ORDER_ID = oio.ORDER_ID AND oi.LINEITEM_ID = oio.LINEITEM_ID
GROUP by
    oio.ORDER_ID,oi.USER_ID
ORDER by
    oi.USER_ID
),
PercentRank AS(
SELECT 
    ORDER_ID,
    USER_ID,
    rev_per_order,
    NTILE(5) OVER (ORDER BY rev_per_order DESC) AS percent_rank
FROM
    TotalRevenue
    )
    
SELECT
    ORDER_ID,
    USER_ID,
    rev_per_order,
    (CASE WHEN percent_rank = 1 THEN "High CLV" WHEN
    percent_rank IN (2,3,4) THEN "Medium CLV" 
    ELSE "Low CLV" END) AS CLV_Category
FROM
    PercentRank
'''
SQLQuery_node1787773761209 = sparkSqlQuery(glueContext, query = SqlQuery484, mapping = {"myDataSourceoio":AmazonS3_node1787761479156, "myDataSourceoi":AmazonS3_node1787761503951}, transformation_ctx = "SQLQuery_node1787773761209")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1787773761209, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1787758491393", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (SQLQuery_node1787773761209.count() >= 1):
   SQLQuery_node1787773761209 = SQLQuery_node1787773761209.coalesce(1)
AmazonS3_node1787775267819 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1787773761209, connection_type="s3", format="csv", connection_options={"path": "s3://global-partners-oseikwadwo/gold/customer_lifetime_value/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1787775267819")

job.commit()