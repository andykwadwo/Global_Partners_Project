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
AmazonS3_node1787707077193 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://global-partners-oseikwadwo/bronze/order_items/run-1787688469688-part-r-00000"], "recurse": True}, transformation_ctx="AmazonS3_node1787707077193")

# Script generated for node SQL Query
SqlQuery448 = '''
SELECT *,
       DATE_FORMAT(CREATION_TIME_UTC, 'yyyy-MM-dd') AS formatted_date,
       ROUND(ITEM_PRICE,2) AS rounded_price
FROM myDataSource


'''
SQLQuery_node1787707187009 = sparkSqlQuery(glueContext, query = SqlQuery448, mapping = {"myDataSource":AmazonS3_node1787707077193}, transformation_ctx = "SQLQuery_node1787707187009")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1787707187009, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1787706845577", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (SQLQuery_node1787707187009.count() >= 1):
   SQLQuery_node1787707187009 = SQLQuery_node1787707187009.coalesce(1)
AmazonS3_node1787707381608 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1787707187009, connection_type="s3", format="csv", connection_options={"path": "s3://global-partners-oseikwadwo/silver/order_items/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1787707381608")

job.commit()