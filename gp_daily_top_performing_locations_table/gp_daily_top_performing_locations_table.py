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
AmazonS3_node1787927339966 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://global-partners-oseikwadwo/bronze/date_dim/run-1787687530204-part-r-00000"], "recurse": True}, transformation_ctx="AmazonS3_node1787927339966")

# Script generated for node Amazon S3
AmazonS3_node1787927313171 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://global-partners-oseikwadwo/silver/order_items/run-1787708185380-part-r-00000"], "recurse": True}, transformation_ctx="AmazonS3_node1787927313171")

# Script generated for node Join
Join_node1787927379141 = Join.apply(frame1=AmazonS3_node1787927339966, frame2=AmazonS3_node1787927313171, keys1=["date_key"], keys2=["formatted_date"], transformation_ctx="Join_node1787927379141")

# Script generated for node SQL Query
SqlQuery552 = '''
select
    RESTAURANT_ID,
    day_of_week,
    ROUND(SUM(ITEM_PRICE * ITEM_QUANTITY),2) AS total_rev_per_day,
    ROUND(AVG(ITEM_PRICE * ITEM_QUANTITY),2) AS avg_rev_per_day,
    COUNT(DISTINCT ORDER_ID) AS order_count_per_day
from
    myDataSource
GROUP BY 
    RESTAURANT_ID,day_of_week
ORDER BY
    RESTAURANT_ID,day_of_week


'''
SQLQuery_node1787927419799 = sparkSqlQuery(glueContext, query = SqlQuery552, mapping = {"myDataSource":Join_node1787927379141}, transformation_ctx = "SQLQuery_node1787927419799")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1787927419799, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1787925458035", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (SQLQuery_node1787927419799.count() >= 1):
   SQLQuery_node1787927419799 = SQLQuery_node1787927419799.coalesce(1)
AmazonS3_node1787928067770 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1787927419799, connection_type="s3", format="csv", connection_options={"path": "s3://global-partners-oseikwadwo/gold/weekday-top-performing-locations/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1787928067770")

job.commit()