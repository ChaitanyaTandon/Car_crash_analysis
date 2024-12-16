from pyspark.sql import SparkSession
from .loader import Loader
from .job import Job


class JobBuilder():
    """
        JobBuilder with options
    """
    def __init__(self,path,spark:SparkSession,question_data_mapping,logger):
        self.spark = spark
        self.path = path
        self.question_data_mapping = question_data_mapping
        self.logger = logger
        
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
                try:
                    files_used = self.question_data_mapping["data_files_used"]
                    threshold = self.question_data_mapping["threshold"]
                    print("Flow in question Id 1 -----------------------")
                    df_1 = Loader(spark=self.spark).readCsvFile(path=self.path + files_used[0])
                    result = Job().get_males_killed_above_threshold(df=df_1,threshold = threshold)
                except Exception as e:
                    self.logger.log_error(e)
                return result

            elif questionId == 2:
                try:
                    files_used = self.question_data_mapping["data_files_used"]
                    print("Flow in question Id 2 -----------------------")
                    df_1 = Loader(spark=self.spark).readCsvFile(path=self.path + files_used[0])
                    result = Job().two_wheelers_booked(df=df_1)
                except Exception as e:
                    self.logger.log_error(e)
                return result

            elif questionId == 3:
                try:
                    files_used = self.question_data_mapping["data_files_used"]
                    print("Flow in question Id 3 -----------------------")
                    df_1 = Loader(spark=self.spark).readCsvFile(path=self.path + files_used[0])
                    df_2 = Loader(spark=self.spark).readCsvFile(path=self.path + files_used[1])
                    result = Job().top_5_car_brands_airbags_not_deployed(df_1=df_1,df_2=df_2)
                except Exception as e:
                    self.logger.log_error(e)
                return result

            elif questionId == 4:
                try:
                    files_used = self.question_data_mapping["data_files_used"]
                    print("Flow in question Id 4 -----------------------")
                    df_1 = Loader(spark=self.spark).readCsvFile(path=self.path + files_used[0])
                    df_2 = Loader(spark=self.spark).readCsvFile(path=self.path + files_used[1])
                    result = Job().valid_license_hit_and_run(df_1=df_1,df_2=df_2)
                except Exception as e:
                    self.logger.log_error(e)
                return result

            elif questionId == 5:
                try:
                    files_used = self.question_data_mapping["data_files_used"]
                    print("Flow in question Id 5 -----------------------")
                    df_1 = Loader(spark=self.spark).readCsvFile(path=self.path + files_used[0])
                    result = Job().noFemale_invoved(df_1=df_1)
                except Exception as e:
                    self.logger.log_error(e)
                return result

            elif questionId == 6:
                try:
                    files_used = self.question_data_mapping["data_files_used"]
                    print("Flow in question Id 6 -----------------------")
                    df_1 = Loader(spark=self.spark).readCsvFile(path=self.path + files_used[0])
                    result = Job().states_with_highest_injuries(df_1=df_1)
                except Exception as e:
                    self.logger.log_error(e)
                return result

            elif questionId == 7:
                try:
                    files_used = self.question_data_mapping["data_files_used"]
                    print("Flow in question Id 7 -----------------------")
                    df_1 = Loader(spark=self.spark).readCsvFile(path=self.path + files_used[0])
                    df_2 = Loader(spark=self.spark).readCsvFile(path=self.path + files_used[1])
                    result = Job().top_ethenic_user_group(df_1=df_1,df_2=df_2)
                except Exception as e:
                    self.logger.log_error(e)
                return result

            elif questionId == 8:
                try:
                    files_used = self.question_data_mapping["data_files_used"]
                    print("Flow in question Id 8 -----------------------")
                    df_1 = Loader(spark=self.spark).readCsvFile(path=self.path + files_used[0])
                    result = Job().top_alcohol_influenced_pincodes(df_1=df_1)
                except Exception as e:
                    self.logger.log_error(e)
                return result

            elif questionId == 9:
                try:
                    files_used = self.question_data_mapping["data_files_used"]
                    print("Flow in question Id 9 -----------------------")
                    df_1 = Loader(spark=self.spark).readCsvFile(path=self.path + files_used[0])
                    df_2 = Loader(spark=self.spark).readCsvFile(path=self.path + files_used[1])
                    df_3 = Loader(spark=self.spark).readCsvFile(path=self.path + files_used[2])
                    result = Job().no_damaged_property_car_insured(df_1=df_1,df_2=df_2,df_3=df_3)
                except Exception as e:
                    self.logger.log_error(e)
                return result

            elif questionId == 10:
                try:
                    files_used = self.question_data_mapping["data_files_used"]
                    print("Flow in question Id 10 -----------------------")
                    df_1 = Loader(spark=self.spark).readCsvFile(path=self.path + files_used[0])
                    df_2 = Loader(spark=self.spark).readCsvFile(path=self.path + files_used[1])
                    df_3 = Loader(spark=self.spark).readCsvFile(path=self.path + files_used[2])
                    result = Job().top_vehicle_makers(df_1=df_1,df_2=df_2,df_3=df_3)
                except Exception as e:
                    self.logger.log_error(e)
                return result

        except Exception as e:
            self.logger.log_critical(e)
