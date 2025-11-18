from pyspark.sql.functions import col
from pyspark.sql import SparkSession
import time

spark = SparkSession \
        .builder \
        .appName('dataframe_cache') \
        .getOrCreate()

print(f'spark application start')
company_emp_path = 'hdfs:///home/spark/sample/linkedin_jobs/companies/employee_counts.csv'
company_emp_schema = 'company_id LONG,employee_count LONG,follower_count LONG,time_recorded TIMESTAMP'
company_ind_path = 'hdfs:///home/spark/sample/linkedin_jobs/companies/company_industries.csv'
company_ind_schema = 'company_id LONG, industry STRING'

# employee_counts Load
company_emp_df = spark.read \
                 .option('header','true') \
                 .option('multiLine','true') \
                 .schema(company_emp_schema) \
                 .csv(company_emp_path)
company_emp_df.persist()
emp_cnt = company_emp_df.count()
print(f'company_emp_df count: {emp_cnt}')

# employee_counts 중복 제거
company_emp_dedup_df = company_emp_df.dropDuplicates(['company_id'])
emp_dedup_cnt = company_emp_dedup_df.count()
print(f'company_emp_dedup_df count: {emp_dedup_cnt}')

# company_industries Load
company_idu_df = spark.read \
                 .option('header','true') \
                 .option('multiLine','true') \
                 .schema(company_ind_schema) \
                 .csv(company_ind_path)
company_idu_df.persist()
idu_cnt = company_idu_df.count()
print(f'company_idu_df count: {idu_cnt}')

company_it_df = company_idu_df.filter(col('industry') == 'IT Services and IT Consulting')

company_emp_cnt_df = company_emp_dedup_df.join(
    other=company_it_df,
    on='company_id',
    how='inner'
).select('company_id', 'employee_count') \
    .sort('employee_count',ascending=False)

company_emp_cnt_df.show()
time.sleep(300)

# 나의 예시 DataFrame API
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, IntegerType, StringType, StructField
# from pyspark.sql.functions import col
# import time;
#
# spark = SparkSession \
#         .builder \
#         .appName('dataframe_cache') \
#         .getOrcreate()
#
# path1 = 'hdfs:///home/spark/sample/linkedin_jobs/companies/company_industries.csv'
# schema1 = StructType([
#     StructField('company_id', IntegerType(), False),
#     StructField('industry', StringType(), True)
# ])
#
# path2 = 'hdfs:///home/spark/sample/linkedin_jobs/companies/employee_counts.csv'
# schema2 = StructType([
#     StructField('company_id', IntegerType(), False),
#     StructField('employee_count', IntegerType(), True),
#     StructField('follower_count', IntegerType(), True),
#     StructField('time_recorded', IntegerType(), True),
# ])
#
# ci_df = spark \
#         .read \
#         .option("header", 'true') \
#         .option('multiLine','true') \
#         .schema(schema1) \
#         .csv(path1)
#
# ec_df = spark \
#         .read \
#         .option("header", 'true') \
#         .option('multiLine','true') \
#         .schema(schema2) \
#         .csv(path2)
#
# # 회사별 산업 도메인 수
# print(ci_df.count())
#
# # 회사별 종업원 수
# print(ec_df.count())
#
#
# filtered_ci = ci_df \
#                .filter(col('industry') == 'IT Services and IT Consulting') \
#                .persist()
#
# dedup_ec = ec_df \
#             .dropDuplicates(['company_id']) \
#             .persist()
#
# big_companies = dedup_ec.filter(col('employee_count') >= 1000)
#
# joined_df = filtered_ci.join(
#         other=big_companies,
#         on='company_id',
#         how='inner'
# ).select('company_id','employee_count') \
#     .sort('employee_count',ascending=False)
#
# joined_df.show()
# time.sleep(300)