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
AmazonS3_node1787844044653 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://global-partners-oseikwadwo/silver/order_items/run-1787708185380-part-r-00000"], "recurse": True}, transformation_ctx="AmazonS3_node1787844044653")

# Script generated for node SQL Query
SqlQuery406 = '''
WITH Raw_RFM_Metrics AS (
    SELECT 
        user_id,
        DATEDIFF(day, MAX(formatted_date), current_date) as recency_days,
        COUNT(DISTINCT CASE 
            WHEN formatted_date >= DATEADD(month, -12, current_date) THEN order_id 
        END) as frequency_count,
        SUM(CASE 
            WHEN formatted_date >= DATEADD(month, -12, current_date) THEN ITEM_PRICE 
            ELSE 0 
        END) as monetary_value
    FROM myDataSource
    GROUP BY user_id
),
RFM_Scores AS (
    SELECT 
        user_id,
        recency_days,
        frequency_count,
        monetary_value,
        NTILE(5) OVER (ORDER BY recency_days ASC) as r_score,
        NTILE(5) OVER (ORDER BY frequency_count DESC) as f_score,
        NTILE(5) OVER (ORDER BY monetary_value DESC) as m_bucket
    FROM Raw_RFM_Metrics
)

SELECT 
    user_id,
    recency_days,
    frequency_count,
    monetary_value,
    r_score,
    f_score,
    m_bucket as monetary_score,

    CASE 
        WHEN m_bucket IN (1,2,3) and r_score IN (1,2,3) and f_score IN (1,2,3) THEN 'VIP'
        WHEN f_score IN (4,5,6) and r_score IN (1,2,3) THEN 'New Customers'
        WHEN f_score IN (4,5,6) and r_score IN (4,5,6) THEN  'Churn Risk' ELSE "None"
    END as clv_tier
FROM RFM_Scores
ORDER BY monetary_value DESC;
'''
SQLQuery_node1787844250293 = sparkSqlQuery(glueContext, query = SqlQuery406, mapping = {"myDataSource":AmazonS3_node1787844044653}, transformation_ctx = "SQLQuery_node1787844250293")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1787844250293, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1787843854341", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
if (SQLQuery_node1787844250293.count() >= 1):
   SQLQuery_node1787844250293 = SQLQuery_node1787844250293.coalesce(1)
AmazonS3_node1787845799866 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1787844250293, connection_type="s3", format="csv", connection_options={"path": "s3://global-partners-oseikwadwo/gold/customer-RFM-Metrics/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1787845799866")

job.commit()