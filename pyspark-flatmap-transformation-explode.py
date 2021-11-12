import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

arrayData = [
    ("Robert,,Williams", ['Java', 'Scala'], {'hair': 'black', 'eye': 'brown'}),
    ('Michael', ['Spark', 'Java', None], {'hair': 'brown', 'eye': None}),
    ('Robert', ['CSharp', ''], {'hair': 'red', 'eye': ''}),
    ('Washington', None, None),
    ('Jefferson', ['1', '2'], {})
]
df = spark.createDataFrame(data=arrayData, schema=['name', 'knownLanguages', 'properties'])
from pyspark.sql.functions import split

df.select(split("name", ",")).show()
df2 = df.select(df.name, explode(df.knownLanguages), df.properties)
df2.select(df.name, df2.col, explode(df2.properties)).show()
# df2.printSchema()
# df2.show()
