from pyspark.sql import SparkSession
from pyspark.sql.functions import col,broadcast, count
import time

spark = SparkSession \
        .builder \
        .appName('driver_and_executor') \
        .config('spark.sql.adaptive.enabled', 'false') \
        .config('spark.executor.memory', '2g') \
        .config('spark.executor.instances', '3') \
        .config('spark.executor.cores', '2') \
        .getOrCreate()

print(f'spark application start')
job_path = 'hdfs:///home/spark/sample/linkedin_jobs/jobs/job_skills.csv'
job_schema = 'job_id LONG, skill_abr STRING'

skill_path = 'hdfs:///home/spark/sample/linkedin_jobs/mappings/skills.csv'
skill_schema = 'skill_abr STRING, skill_name STRING'

# job Load
job_df = spark.read \
         .option('header','true') \
         .option('multiLine','true') \
         .schema(job_schema) \
         .csv(job_path)

skill_df = spark.read \
         .option('header','true') \
         .option('multiLine','true') \
         .schema(skill_schema) \
         .csv(skill_path)

result_df = job_df.join(
    other=broadcast(skill_df),
    on='skill_abr',
    how='inner'
) \
.groupby('skill_name') \
.agg(count('job_id').alias('job_count')) \
.orderBy(col('job_count').desc())

result_df.show()

# 최종 데이터프레임 건수 확인
print(f'result_df count: {result_df.count()}')

time.sleep(1200)