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

            elif questionId == 4:
                files_used = self.question_data_mapping["data_files_used"]
                print("Flow in question Id 4 -----------------------")
                df_1 = Loader(spark=self.spark).readCsvFile(path=self.path + files_used[0])
                df_2 = Loader(spark=self.spark).readCsvFile(path=self.path + files_used[1])
                result = Job().valid_license_hit_and_run(df_1=df_1,df_2=df_2)
                return result

            elif questionId == 5:
                files_used = self.question_data_mapping["data_files_used"]
                print("Flow in question Id 5 -----------------------")
                df_1 = Loader(spark=self.spark).readCsvFile(path=self.path + files_used[0])
                result = Job().noFemale_invoved(df_1=df_1)
                return result

            elif questionId == 6:
                files_used = self.question_data_mapping["data_files_used"]
                print("Flow in question Id 6 -----------------------")
                df_1 = Loader(spark=self.spark).readCsvFile(path=self.path + files_used[0])
                result = Job().states_with_highest_injuries(df_1=df_1)
                return result

            elif questionId == 7:
                files_used = self.question_data_mapping["data_files_used"]
                print("Flow in question Id 7 -----------------------")
                df_1 = Loader(spark=self.spark).readCsvFile(path=self.path + files_used[0])
                df_2 = Loader(spark=self.spark).readCsvFile(path=self.path + files_used[1])
                result = Job().top_ethenic_user_group(df_1=df_1,df_2=df_2)
                return result

            elif questionId == 8:
                files_used = self.question_data_mapping["data_files_used"]
                print("Flow in question Id 8 -----------------------")
                df_1 = Loader(spark=self.spark).readCsvFile(path=self.path + files_used[0])
                result = Job().top_alcohol_influenced_pincodes(df_1=df_1)
                return result

            elif questionId == 9:
                files_used = self.question_data_mapping["data_files_used"]
                print("Flow in question Id 9 -----------------------")
                df_1 = Loader(spark=self.spark).readCsvFile(path=self.path + files_used[0])
                df_2 = Loader(spark=self.spark).readCsvFile(path=self.path + files_used[1])
                df_3 = Loader(spark=self.spark).readCsvFile(path=self.path + files_used[2])
                result = Job().no_damaged_property_car_insured(df_1=df_1,df_2=df_2,df_3=df_3)
                return result

        except:
            pass
