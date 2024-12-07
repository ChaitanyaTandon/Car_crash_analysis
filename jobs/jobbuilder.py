from pyspark.sql import SparkSession
from .loader import Loader
from .job import Job


class JobBuilder():
    """
        JobBuilder with options
    """
    def __init__(self,path,spark:SparkSession):
        self.spark = spark
        self.path = path
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
                print("Flow in question Id 1 -----------------------")
                result = Job().get_males_killed_above_threshold(df=Loader(spark=self.spark).readCsvFile(path=self.path + "Primary_Person_use.csv"))
                return result
        except:
            pass
