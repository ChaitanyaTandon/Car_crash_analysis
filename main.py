from utils.utils import FileLoader
import json
from pyspark.sql import SparkSession
from jobs.loader import Loader
from jobs.jobbuilder import JobBuilder
import os
from jobs.logger import AppLogger




if __name__ == "__main__":
    
    conf_data = FileLoader().load_json_data("configs/config.json")
    question_data_mapping = FileLoader().load_json_data("configs/questions_data_mapping.json")
    question_data_mapping = question_data_mapping.get(str(conf_data["functions"]["analysis_id"]))
    logger = AppLogger(log_file_name="main.log",log_file_path=os.getcwd()+conf_data["logging_path"])
    logger.log_info(f"App Started with config -> {conf_data}, Question data mapping -> {question_data_mapping}")
    spark = SparkSession.builder.appName(str(question_data_mapping.get("app_name"))).getOrCreate()
    result = JobBuilder(path=conf_data["file_path"]["source_path"],spark=spark,question_data_mapping=question_data_mapping,logger=logger). \
                        extractionData(questionId=conf_data["functions"]["analysis_id"])
    try:
        Loader(spark=spark).writeCsvFile(path=conf_data["file_path"]["destination_path"]+"Analysis " +
                                     str(conf_data["functions"]["analysis_id"]) + "/",
                                     Dataframe=result,mode=conf_data["functions"]["mode"])
    except Exception as e:
        logger.log_critical(e)
    spark.stop()