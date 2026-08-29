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
AmazonS3_node1787925469261 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://global-partners-oseikwadwo/silver/order_items/run-1787708185380-part-r-00000"], "recurse": True}, transformation_ctx="AmazonS3_node1787925469261")

# Script generated for node SQL Query
SqlQuery461 = '''
select
    user_id
    IS_LOYALTY,
    ROUND(AVG(ITEM_PRICE * ITEM_QUANTITY),2) AS average_spent_per_customer,
    COUNT(DISTINCT ORDER_ID) AS order_count_per_customer,
    ROUND(SUM(ITEM_PRICE * ITEM_QUANTITY),2) AS customer_ltv
FROM
    myDataSource
GROUP by
    user_id,IS_LOYALTY
HAVING 
    COUNT(DISTINCT ORDER_ID) > 1
'''
SQLQuery_node1787925537533 = sparkSqlQuery(glueContext, query = SqlQuery461, mapping = {"myDataSource":AmazonS3_node1787925469261}, transformation_ctx = "SQLQuery_node1787925537533")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1787925537533, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1787925458035", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (SQLQuery_node1787925537533.count() >= 1):
   SQLQuery_node1787925537533 = SQLQuery_node1787925537533.coalesce(1)
AmazonS3_node1787925788219 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1787925537533, connection_type="s3", format="csv", connection_options={"path": "s3://global-partners-oseikwadwo/gold/loyalty-program-metrics/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1787925788219")

job.commit()