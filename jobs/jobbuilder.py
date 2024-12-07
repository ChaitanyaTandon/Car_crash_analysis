from pyspark.sql import SparkSession
from .loader import Loader
from .job import Job


class JobBuilder():
    """
        JobBuilder with options
    """
    def __init__(self,path,spark:SparkSession,question_data_mapping):
        self.spark = spark
        self.path = path
        self.question_data_mapping = question_data_mapping
        # self.spark.sparkContext.setLogLevel("ERROR")
        # self.spark.sparkContext.setLogLevel("WARN")
        # self.spark.sparkContext.setLogLevel("INFO")
        #self.spark.sparkContext.setLogLevel("DEBUG")
        
    def extractionData(self, questionId:int):
        """_summary_
            Case Statement for performing the ETL pipeline.
        Args:
            questionId (str): Question identifier for the extraction.
        Returns:
            None 
        """ 
        try:
            if questionId==None:
                return None
            
            elif questionId==1:
                files_used = self.question_data_mapping["data_files_used"]
                threshold = self.question_data_mapping["threshold"]
                print("Flow in question Id 1 -----------------------")
                df_1 = Loader(spark=self.spark).readCsvFile(path=self.path + files_used[0])
                result = Job().get_males_killed_above_threshold(df=df_1,threshold = threshold)
                return result

            elif questionId == 2:
                files_used = self.question_data_mapping["data_files_used"]
                print("Flow in question Id 2 -----------------------")
                df_1 = Loader(spark=self.spark).readCsvFile(path=self.path + files_used[0])
                result = Job().two_wheelers_booked(df=df_1)
                return result

            elif questionId == 3:
                files_used = self.question_data_mapping["data_files_used"]
                print("Flow in question Id 3 -----------------------")
                df_1 = Loader(spark=self.spark).readCsvFile(path=self.path + files_used[0])
                df_2 = Loader(spark=self.spark).readCsvFile(path=self.path + files_used[1])
                result = Job().top_5_car_brands_airbags_not_deployed(df_1=df_1,df_2=df_2)
                return result


        except:
            pass
