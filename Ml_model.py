import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.regression import LinearRegression, GBTRegressor, RandomForestRegressor, DecisionTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import *
from pyspark.ml.feature import StandardScaler
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.types import FloatType


def main(inputs, output):
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

    data = spark.read.option("header",True).csv(inputs, schema=tmax_schema)
    #data.show(truncate = False)
    data_model = data.where(data['Cases'].isNotNull() & data['Tests'].isNotNull() & data['Vaccinations'].isNotNull() & data['Recoveries'].isNotNull() & data['Criticals'].isNotNull() & data['Hospitalizations'].isNotNull() & data['Deaths'].isNotNull() )
    #data2.printSchema()
    
    train, validation = data_model.randomSplit([0.75, 0.25])
    assembler = VectorAssembler(inputCols=["Cases", "Hospitalizations" ,"Vaccinations","Tests","Criticals"], outputCol="features")
   

    scaler =StandardScaler().setInputCol("features").setOutputCol("scaled_features")

    regressor = GBTRegressor(featuresCol = 'scaled_features', labelCol = 'Deaths')

    evaluator = RegressionEvaluator(metricName="rmse", labelCol=regressor.getLabelCol(), predictionCol=regressor.getPredictionCol())
    paramGrid = ParamGridBuilder().addGrid(regressor.maxDepth, [2, 5]).addGrid(regressor.maxIter, [10, 100]).build()
    cv = CrossValidator(estimator=regressor, evaluator=evaluator, estimatorParamMaps=paramGrid)
    



    pipeline = Pipeline(stages=[assembler, scaler, cv])
    model = pipeline.fit(train)
    
    predictions = model.transform(validation)

    

    r2_evaluator = RegressionEvaluator(metricName="r2", labelCol=regressor.getLabelCol(), predictionCol=regressor.getPredictionCol())
    
    r2 = r2_evaluator.evaluate(predictions)
    rmse = evaluator.evaluate(predictions)

    print('r2 =', r2)
    print('rmse =', rmse)


    print("Rmse Train: ", evaluator.evaluate(model.transform(train)))
    print("Rmse Validation: ", evaluator.evaluate(model.transform(validation)))
    print("R2 Train: ", r2_evaluator.evaluate(model.transform(train)))
    print("R2 Validation: ", r2_evaluator.evaluate(model.transform(validation)))



    model.write().overwrite().save(output)

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)