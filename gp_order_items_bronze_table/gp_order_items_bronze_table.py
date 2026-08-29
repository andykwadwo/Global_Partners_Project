import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node Microsoft SQL Server
MicrosoftSQLServer_node1787688188786 = glueContext.create_dynamic_frame.from_options(
    connection_type = "sqlserver",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "order_items",
        "connectionName": "SSMS-SQL-Connection",
    },
    transformation_ctx = "MicrosoftSQLServer_node1787688188786"
)

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=MicrosoftSQLServer_node1787688188786, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1787674963108", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (MicrosoftSQLServer_node1787688188786.count() >= 1):
   MicrosoftSQLServer_node1787688188786 = MicrosoftSQLServer_node1787688188786.coalesce(1)
AmazonS3_node1787688242779 = glueContext.write_dynamic_frame.from_options(frame=MicrosoftSQLServer_node1787688188786, connection_type="s3", format="csv", connection_options={"path": "s3://global-partners-oseikwadwo/bronze/order_items/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1787688242779")

job.commit()