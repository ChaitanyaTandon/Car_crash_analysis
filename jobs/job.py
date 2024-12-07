from pyspark.sql import DataFrame
from pyspark.sql.functions import sum,desc


class Job():
    """Analysis Job Function
    """
    def __init__(self):
        # self.logger = Logger()
        pass
            
    def get_males_killed_above_threshold(self,df,threshold: int)->DataFrame:
        """_summary_
            The No of Occurences of crashes where number of males killed 
            are greater than threshold value, 2 in this case  
        Args:
            personDataframe (DataFrame): Pyspark DataFrame object.
            threshold (int) : 2 in this case (can be changed accordingly)
        Returns:
            DataFrame: Pyspark DataFrame object.
        """
        try:
            df_primary_person_male = df.filter(df.PRSN_GNDR_ID == "MALE")
            
            df_male_death_counts = df_primary_person_male.groupBy("CRASH_ID").agg(sum("DEATH_CNT").alias("total_deaths"))
            print("==============================================")
            crashes_male_deaths_more_than_2 = df_male_death_counts.filter(df_male_death_counts.total_deaths > threshold)
            #crashes_male_deaths_more_than_2 = df_male_death_counts.filter(df_male_death_counts.total_deaths > 2).count()
        except Exception as exception:
            print('Error::{}'.format(exception)+"\n")
        finally:
            return crashes_male_deaths_more_than_2