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
AmazonS3_node1787928424423 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://global-partners-oseikwadwo/bronze/date_dim/run-1787687530204-part-r-00000"], "recurse": True}, transformation_ctx="AmazonS3_node1787928424423")

# Script generated for node Amazon S3
AmazonS3_node1787928399163 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://global-partners-oseikwadwo/silver/order_items/run-1787708185380-part-r-00000"], "recurse": True}, transformation_ctx="AmazonS3_node1787928399163")

# Script generated for node Join
Join_node1787928450805 = Join.apply(frame1=AmazonS3_node1787928424423, frame2=AmazonS3_node1787928399163, keys1=["date_key"], keys2=["formatted_date"], transformation_ctx="Join_node1787928450805")

# Script generated for node SQL Query
SqlQuery401 = '''
select
    RESTAURANT_ID,
    week,
    ROUND(SUM(ITEM_PRICE * ITEM_QUANTITY),2) AS total_rev_per_week,
    ROUND(AVG(ITEM_PRICE * ITEM_QUANTITY),2) AS avg_rev_per_week,
    COUNT(DISTINCT ORDER_ID) AS order_count_per_week
from
    myDataSource
GROUP BY 
    RESTAURANT_ID,week
ORDER BY
    RESTAURANT_ID,week
'''
SQLQuery_node1787928477623 = sparkSqlQuery(glueContext, query = SqlQuery401, mapping = {"myDataSource":Join_node1787928450805}, transformation_ctx = "SQLQuery_node1787928477623")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1787928477623, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1787925458035", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (SQLQuery_node1787928477623.count() >= 1):
   SQLQuery_node1787928477623 = SQLQuery_node1787928477623.coalesce(1)
AmazonS3_node1787928716381 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1787928477623, connection_type="s3", format="csv", connection_options={"path": "s3://global-partners-oseikwadwo/gold/top-performing-weeks-by-location/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1787928716381")

job.commit()