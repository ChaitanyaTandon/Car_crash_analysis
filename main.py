from utils.utils import FileLoader
import json
from pyspark.sql import SparkSession
from jobs.loader import Loader
from jobs.jobbuilder import JobBuilder
import os
from jobs.logger import Logger




if __name__ == "__main__":
    
    conf_data = FileLoader().load_json_data("configs/config.json")
    question_data_mapping = FileLoader().load_json_data("configs/questions_data_mapping.json")
    question_data_mapping = question_data_mapping.get(str(conf_data["functions"]["analysis_id"]))
    LOG = Logger("main",os.getcwd()+conf_data["logging_path"])
    LOG._logger.info(conf_data)
    spark = SparkSession.builder.appName(str(question_data_mapping.get("app_name"))).getOrCreate()
    LOG._logger.info(spark)
    result = JobBuilder(path=conf_data["file_path"]["source_path"],spark=spark,question_data_mapping=question_data_mapping). \
                        extractionData(questionId=conf_data["functions"]["analysis_id"])
    Loader(spark=spark).writeCsvFile(path=conf_data["file_path"]["destination_path"]+"Analysis " +
                                     str(conf_data["functions"]["analysis_id"]) + "/",
                                     Dataframe=result,mode=conf_data["functions"]["mode"])
    spark.stop()