from pyspark.sql import DataFrame
from pyspark.sql.functions import sum,desc,col,count


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
        except Exception as exception:
            print('Error::{}'.format(exception)+"\n")
        finally:
            return crashes_male_deaths_more_than_2


    def two_wheelers_booked(self,df):
        df_units = df
        try:
            count_2_wheelers = df_units.select('VIN').filter((col('VEH_BODY_STYL_ID')).like('%MOTORCYCLE%')).distinct().count()
        except Exception as exception:
            print('Error::{}'.format(exception)+"\n")
        finally:
            return count_2_wheelers


    def top_5_car_brands_airbags_not_deployed(self,df_1,df_2):
        df_primary_person = df_1
        df_units          = df_2

        try:
            df_driver = df_primary_person.filter((col('PRSN_TYPE_ID')).like('DRIVER'))
            df_driver_killed = df_driver.filter((col('PRSN_INJRY_SEV_ID') == 'KILLED'))
            df_airbags_not_deployed = df_driver_killed.filter((col('PRSN_AIRBAG_ID') == 'NOT DEPLOYED'))
            df_airbags_not_deployed_crash_id = df_airbags_not_deployed.select("CRASH_ID")
            df_units = df_units.select("CRASH_ID","VEH_MAKE_ID")
            df_units = df_units.filter(col('DEATH_CNT') == 1)
            df_veh = df_airbags_not_deployed_crash_id.join(df_units,df_airbags_not_deployed_crash_id.CRASH_ID == df_units.CRASH_ID,how="left")
            grouped_df = df_veh.groupBy("VEH_MAKE_ID").agg(count("*").alias("count")).orderBy(desc("count")).limit(5)
        
        except Exception as exception:
            print('Error::{}'.format(exception)+"\n")
        finally:
            return grouped_df