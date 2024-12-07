from pyspark.sql import DataFrame
from pyspark.sql.functions import sum,desc,col,count,rank,regexp_extract,row_number
from pyspark.sql.window import Window


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

    def valid_license_hit_and_run(self,df_1,df_2):
        df_primary_person = df_1
        df_units          = df_2

        try:
            df_primary_person = df_primary_person.dropna(subset=["DRVR_LIC_CLS_ID"])
            exclude_list = ['OTHER/OUT OF STATE','UNKNOWN','UNLICENSED']
            df_primary_person_valid_license = df_primary_person.filter(~col('DRVR_LIC_CLS_ID').isin(exclude_list))
            df_units_hnr = df_units.filter(col('VEH_HNR_FL')== 'Y')
            df_hnr_crash_id  = df_units_hnr.select('CRASH_ID').dropDuplicates(subset=["CRASH_ID"])
            df_valid_lic_hnr= df_primary_person_valid_license.join(df_hnr_crash_id,df_primary_person_valid_license.CRASH_ID==df_hnr_crash_id.CRASH_ID,how='leftsemi')
            row_count = df_valid_lic_hnr.count()
            
        
        except Exception as exception:
            print('Error::{}'.format(exception)+"\n")
        finally:
            return row_count

    def noFemale_invoved(self,df_1):
        df_primary_person = df_1

        try:
            df_females_not_involed = df_primary_person.filter(col('PRSN_GNDR_ID') != "FEMALE").groupBy("DRVR_LIC_STATE_ID").agg(count("*").alias("count")).orderBy(desc("count")).limit(1)
        except Exception as exception:
            print('Error::{}'.format(exception)+"\n")
        finally:
            return df_females_not_involed

    def states_with_highest_injuries(self,df_1):
        df_units = df_1

        try:
            df_top_5_injury_count = df_units.groupBy("VEH_MAKE_ID").agg(sum("TOT_INJRY_CNT").alias("Total_injury_count")).orderBy(desc("Total_injury_count")).limit(5)
            window_spec = Window.orderBy(df_top_5_injury_count["Total_injury_count"].desc())
            ranked_df = df_top_5_injury_count.withColumn("rank", rank().over(window_spec))
            ranked_df = ranked_df.filter(col('rank')>=3)
        except Exception as exception:
            print('Error::{}'.format(exception)+"\n")
        finally:
            return ranked_df

    def top_ethenic_user_group(self,df_1,df_2):
        df_primary_person = df_1
        df_units = df_2

        try:
            ethnic_df = df_primary_person.filter("PRSN_ETHNICITY_ID != 'NA' and PRSN_ETHNICITY_ID != 'UNKNOWN' and PRSN_ETHNICITY_ID != 'OTHER'").select('CRASH_ID','UNIT_NBR','PRSN_ETHNICITY_ID')
            body_style_df = df_units.filter("VEH_BODY_STYL_ID != 'NA' and VEH_BODY_STYL_ID != 'UNKNOWN' and VEH_BODY_STYL_ID != 'NOT REPORTED'").filter(~col('VEH_BODY_STYL_ID').like('OTHER%')).select('CRASH_ID','UNIT_NBR','VEH_BODY_STYL_ID')
            combined_df = body_style_df.join(ethnic_df,on=['CRASH_ID','UNIT_NBR'],how='inner')
            grouped_df = combined_df.groupBy('VEH_BODY_STYL_ID','PRSN_ETHNICITY_ID').count() 
            window_spec = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(grouped_df["count"].desc())
            ranked_df = grouped_df.withColumn("rank", rank().over(window_spec))
            ranked_df = ranked_df.filter(col('rank')==1)
            ranked_df = ranked_df.drop('rank')
        except Exception as exception:
            print('Error::{}'.format(exception)+"\n")
        finally:
            return ranked_df

    
    def top_alcohol_influenced_pincodes(self,df_1):
        df_primary_person = df_1

        try:
            df_alcohol_influenced = df_primary_person.filter("PRSN_ALC_RSLT_ID == 'Positive' and DRVR_ZIP is not null").select('DRVR_ZIP').groupBy('DRVR_ZIP').count()
            df_alcohol_influenced_top_5 = df_alcohol_influenced.orderBy(df_alcohol_influenced["count"].desc()).limit(5)
            df_alcohol_influenced_top_5 = df_alcohol_influenced_top_5.withColumnRenamed('count',"Crashes")

        except Exception as exception:
            print('Error::{}'.format(exception)+"\n")
        finally:
            return df_alcohol_influenced_top_5

    def no_damaged_property_car_insured(self,df_1,df_2,df_3):
        df_charges = df_1
        df_damages = df_2
        df_units = df_3

        try:
            no_insurance_df = df_charges.filter(col('CHARGE').contains('NO')).filter(col('CHARGE').contains('INSURANCE')).select('CRASH_ID','UNIT_NBR').withColumnRenamed('CRASH_ID','I_CRASH_ID').withColumnRenamed('UNIT_NBR','I_UNIT_NBR')
            damaged_df = df_damages.select('CRASH_ID').distinct().withColumnRenamed('CRASH_ID','D_CRASH_ID')
            joined_df= df_units.join(no_insurance_df,(df_units['CRASH_ID']==no_insurance_df['I_CRASH_ID']) & (df_units['UNIT_NBR']==no_insurance_df['I_UNIT_NBR']),how='left')
            units_joined_df = joined_df.filter("I_CRASH_ID is null").select('CRASH_ID','UNIT_NBR','VEH_DMAG_SCL_1_ID','VEH_DMAG_SCL_2_ID')
            damaged_df_combined = units_joined_df.join(damaged_df,units_joined_df['CRASH_ID']==damaged_df['D_CRASH_ID'],how='left')
            combined= damaged_df_combined.filter("D_CRASH_ID is null").select('CRASH_ID','VEH_DMAG_SCL_1_ID','VEH_DMAG_SCL_2_ID')
            result = combined.withColumn('DMAG1_RANGE',regexp_extract(col('VEH_DMAG_SCL_1_ID'), "\\d+", 0)) \
                             .withColumn('DMAG2_RANGE',regexp_extract(col('VEH_DMAG_SCL_2_ID'), "\\d+", 0)) \
                             .filter("DMAG1_RANGE > 4 or DMAG2_RANGE > 4") \
                             .select('CRASH_ID').distinct().count()
            print(result)
        except Exception as exception:
            print('Error::{}'.format(exception)+"\n")
        finally:
            return result


    def top_vehicle_makers(self,df_1,df_2,df_3):
        df_charges = df_1
        df_units = df_2
        df_primary_person = df_3

        try:
            speed_charge_df = df_charges.filter(col('CHARGE').contains('SPEED')).select('CRASH_ID','UNIT_NBR','CHARGE') 
            top_col_df = df_units.filter("VEH_COLOR_ID != 'NA'").select('VEH_COLOR_ID').groupBy('VEH_COLOR_ID').count()
            top_col_df = top_col_df.withColumn('rn',row_number().over(Window.orderBy(col('count').desc()))).filter("rn <=10").select('VEH_COLOR_ID')
            top_states_df = df_units.filter(~col('VEH_LIC_STATE_ID').isin(['NA'])).select('VEH_LIC_STATE_ID').groupBy('VEH_LIC_STATE_ID').count()
            top_states_df = top_states_df.withColumn('rn',row_number().over(Window.orderBy(col('count').desc()))).filter("rn <=25").select('VEH_LIC_STATE_ID')
            subjoin = df_units.join(top_col_df,on=['VEH_COLOR_ID'],how='inner').select('CRASH_ID','UNIT_NBR','VEH_MAKE_ID','VEH_LIC_STATE_ID')
            chargesubjoin = subjoin.join(speed_charge_df,on=['CRASH_ID','UNIT_NBR'],how='inner').select('CRASH_ID','UNIT_NBR','VEH_MAKE_ID','VEH_LIC_STATE_ID')
            combined_df = chargesubjoin.join(top_states_df,on=['VEH_LIC_STATE_ID'],how='inner').select('CRASH_ID','UNIT_NBR','VEH_MAKE_ID')
            licensed_df = df_primary_person.filter("DRVR_LIC_CLS_ID != 'UNLICENSED'").select('CRASH_ID','UNIT_NBR')
            final_df = combined_df.join(licensed_df,on=['CRASH_ID','UNIT_NBR'],how='inner').select('CRASH_ID','VEH_MAKE_ID').groupBy('VEH_MAKE_ID').count() 
            final_df = final_df.withColumn('rn',row_number().over(Window.orderBy(col('count').desc()))).filter("rn <= 5").select('VEH_MAKE_ID')

            
        except Exception as exception:
            print('Error::{}'.format(exception)+"\n")
        finally:
            return final_df