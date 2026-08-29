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
AmazonS3_node1787861896359 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://global-partners-oseikwadwo/silver/order_items/run-1787708185380-part-r-00000"], "recurse": True}, transformation_ctx="AmazonS3_node1787861896359")

# Script generated for node Amazon S3
AmazonS3_node1787861790152 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://global-partners-oseikwadwo/bronze/date_dim/run-1787687530204-part-r-00000"], "recurse": True}, transformation_ctx="AmazonS3_node1787861790152")

# Script generated for node Join
Join_node1787862050631 = Join.apply(frame1=AmazonS3_node1787861896359, frame2=AmazonS3_node1787861790152, keys1=["formatted_date"], keys2=["date_key"], transformation_ctx="Join_node1787862050631")

# Script generated for node SQL Query
SqlQuery500 = '''
select
    RESTAURANT_ID,
    ITEM_CATEGORY,
    week,
    ROUND(SUM(ITEM_PRICE * ITEM_QUANTITY),2) AS weekly_rev
from
    myDataSource
GROUP by
    RESTAURANT_ID,ITEM_CATEGORY,week
ORDER by
    RESTAURANT_ID
'''
SQLQuery_node1787862176747 = sparkSqlQuery(glueContext, query = SqlQuery500, mapping = {"myDataSource":Join_node1787862050631}, transformation_ctx = "SQLQuery_node1787862176747")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1787862176747, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1787843854341", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (SQLQuery_node1787862176747.count() >= 1):
   SQLQuery_node1787862176747 = SQLQuery_node1787862176747.coalesce(1)
AmazonS3_node1787862393484 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1787862176747, connection_type="s3", format="csv", connection_options={"path": "s3://global-partners-oseikwadwo/gold/customer-weekly-rev/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1787862393484")

job.commit()