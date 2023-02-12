import os
import sys
from awsglue.job import Job
from awsglue.transforms import *
from pyspark.sql.types import LongType
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as func
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# declaring constant variables
BUCKET_NAME = "api-to-s3-data"
DYNAMODB_TABLE_NAME = "finance_data"
INPUT_FILE_PATH = f"s3://{BUCKET_NAME}/inbox/*json"

# getting logger object to log the progress
logger = glueContext.get_logger()

logger.info(f"Starting reading json file from {INPUT_FILE_PATH}")
df_sparkdf = spark.read.json(INPUT_FILE_PATH)
logger.info(f"Type casting the columns of spark dataframe to Long type")
df_sparkdf = df_sparkdf.withColumn('complaint_id', func.col('complaint_id').cast(LongType()))

logger.info(f"Columns in dataframe: {len(df_sparkdf.columns)}---> {df_sparkdf.columns}")
logger.info(f"NUmber of rows found in file : {df_sparkdf.count()}")

dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="dynamodb",
    connection_options={"dynamodb.input.tableName": DYNAMODB_TABLE_NAME,
                        "dynamodb.throughput.read.percent": "1.0",
                        "dynamodb.splits": "100"
                        }
)

dyf_sparkdf = dyf.toDF()
new_sparkdf = None
if dyf_sparkdf.count() != 0:
    logger.info(f"Columns in dataframe: {len(dyf_sparkdf.columns)}---> {dyf_sparkdf.columns}")
    logger.info(f"Number of rows found in file : {dyf_sparkdf.count()}")
    logger.info(f"Renaming existing complaint id columns of dynamodb")
    existing_complaint_spark_df = dyf_sparkdf.withColumnRenamed('complaint_id', "existing_complaint_id")
    logger.info(f"Applying left join on new dataframe from s3 and dynamo db")
    joined_sparkdf = df_sparkdf.join(existing_complaint_spark_df,
                                     df_sparkdf.complaint_id == existing_complaint_spark_df.existing_complaint_id,
                                     "left")
    logger.info(f"Number of rows after left join: {joined_sparkdf.count()}")
    new_sparkdf = joined_sparkdf.filter("existing_complaint_id is null")
    new_sparkdf.drop('existing_complaint_id')
    new_sparkdf = new_sparkdf.coalesce(10)
else:
    new_sparkdf = df_sparkdf.coalesce(10)

logger.info(f"Converting spark dataframe to DynamicFrame")
newDynamicFrame = DynamicFrame.fromDF(new_sparkdf, glueContext, "new_sparkdf")
logger.info(f"Starting writing new records into dyna db dataframe.")
logger.info(f"Number of records will be written to dynamodb: {new_sparkdf.count()}")

glueContext.write_dynamic_frame_from_options(
    frame=newDynamicFrame,
    connection_type="dynamodb",
    connection_options={
        "dynamodb.output.tableName": DYNAMODB_TABLE_NAME,
        "dynamodb.throughput.write.percent": "1.0"
    }
)
logger.info(f"Data has been dumped into dynamodb")
logger.info(f"Archiving file from input source: s3://{BUCKET_NAME}/inbox to archive: s3://{BUCKET_NAME}/archive")
os.system(f"aws s3 sync s3://{BUCKET_NAME}/inbox to archive: s3://{BUCKET_NAME}/archive")

logger.info(f"File is successfully archieved.")
os.system(f"aws s3 rm s3://{BUCKET_NAME}/inbox/ --recursive")

job.commit()