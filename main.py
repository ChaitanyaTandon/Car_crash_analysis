from utils.utils import FileLoader
import json
from pyspark.sql import SparkSession
from jobs.loader import Loader
from jobs.jobbuilder import JobBuilder
import os

# spark = SparkSession.builder.appName("test_01").getOrCreate()
# print(spark)



if __name__ == "__main__":
    
    conf_data = FileLoader().load_json_data("configs/config.json")
    question_data_mapping = FileLoader().load_json_data("configs/questions_data_mapping.json")
    spark = SparkSession.builder.appName("Analysis").getOrCreate()
    result = JobBuilder(path=conf_data["file_path"]["source_path"],spark=spark).extractionData(questionId=conf_data["functions"]["analysis_id"])
    result.show()
    Loader(spark=spark).writeCsvFile(path=conf_data["file_path"]["destination_path"]+"Analysis " +
                                     str(conf_data["functions"]["analysis_id"]) + "/",
                                     Dataframe=result,mode=conf_data["functions"]["mode"])
    spark.stop()