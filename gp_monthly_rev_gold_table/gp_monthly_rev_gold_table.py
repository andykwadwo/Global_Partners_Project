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
AmazonS3_node1787863561293 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://global-partners-oseikwadwo/bronze/date_dim/run-1787687530204-part-r-00000"], "recurse": True}, transformation_ctx="AmazonS3_node1787863561293")

# Script generated for node Amazon S3
AmazonS3_node1787863698143 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://global-partners-oseikwadwo/silver/order_items/run-1787708185380-part-r-00000"], "recurse": True}, transformation_ctx="AmazonS3_node1787863698143")

# Script generated for node Join
Join_node1787863830948 = Join.apply(frame1=AmazonS3_node1787863698143, frame2=AmazonS3_node1787863561293, keys1=["formatted_date"], keys2=["date_key"], transformation_ctx="Join_node1787863830948")

# Script generated for node SQL Query
SqlQuery483 = '''
select
    RESTAURANT_ID,
    ITEM_CATEGORY,
    month,
    ROUND(SUM(ITEM_PRICE * ITEM_QUANTITY),2) AS monthly_rev
from
    myDataSource
GROUP by
    RESTAURANT_ID,ITEM_CATEGORY,month
ORDER by
    RESTAURANT_ID
'''
SQLQuery_node1787863992082 = sparkSqlQuery(glueContext, query = SqlQuery483, mapping = {"myDataSource":Join_node1787863830948}, transformation_ctx = "SQLQuery_node1787863992082")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1787863992082, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1787843854341", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (SQLQuery_node1787863992082.count() >= 1):
   SQLQuery_node1787863992082 = SQLQuery_node1787863992082.coalesce(1)
AmazonS3_node1787864285923 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1787863992082, connection_type="s3", format="csv", connection_options={"path": "s3://global-partners-oseikwadwo/gold/customer-monthly-rev/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1787864285923")

job.commit()