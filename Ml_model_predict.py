import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('tmax model tester').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import PipelineModel
import datetime
from pyspark.ml.evaluation import RegressionEvaluator

def test_model(model_file, output):
    tmax_schema = types.StructType([
    types.StructField('Date', types.StringType()),
    types.StructField('Cases', types.FloatType()),
    types.StructField('Tests', types.FloatType()),
    types.StructField('Vaccinations', types.FloatType()),
    types.StructField('Recoveries', types.FloatType()),
    types.StructField('Criticals', types.FloatType()),
    types.StructField('Hospitalizations', types.FloatType()),
    types.StructField('Deaths', types.FloatType()),
    ])

    someData = [(datetime.date(2021,11,29),2324.0,85558.0,1160943.0,200.0,471.0,1556.0,0.0)]
    
    someDF = spark.createDataFrame(someData, schema = tmax_schema)

    # load the model
    model = PipelineModel.load(model_file)

    predictions = model.transform(someDF)
    predictions.show()

    final = predictions.select('prediction')
    final.coalesce(1).write.json(output)


if __name__ == '__main__':
    model_file = sys.argv[1]
    output = sys.argv[2]
    test_model(model_file,output)