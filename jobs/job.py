from pyspark.sql import DataFrame
from pyspark.sql.functions import sum,desc,col,count,rank,regexp_extract,row_number,countDistinct
from pyspark.sql.window import Window


class Job():
    """
        Analysis Job Function
    """
    def __init__(self):
        pass
        
            
    def get_males_killed_above_threshold(self,df,threshold: int)->DataFrame:
        """      
        
        Identifies crashes where the number of males killed exceeds a specified threshold.

        Parameters:
        ----------
        df : pyspark.sql.DataFrame
            Input DataFrame containing primary person information with columns `PRSN_GNDR_ID` and `DEATH_CNT`.
        
        threshold : int
            The threshold value for the number of deaths (e.g., 2 in this case).
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


    def two_wheelers_booked(self, df: DataFrame) -> DataFrame:
            """
            Computes the count of two wheelers are booked for crashes

            Parameters:
            ----------
            df : pyspark.sql.DataFrame
                Input DataFrame containing vehicle booking information with columns
                including `VIN` and `VEH_BODY_STYL_ID`.

            Returns:
            -------
            pyspark.sql.DataFrame
                A DataFrame containing a single row and column (`count`) with the count
                of distinct two-wheeler vehicles.

            Raises:
            ------
            ValueError:
                If the input DataFrame is empty or missing required columns.
            Exception:
                For any unexpected errors during processing.
            """
            try:
                # Validate that required columns exist in the input DataFrame
                required_columns = {"VIN", "VEH_BODY_STYL_ID"}
                if not required_columns.issubset(df.columns):
                    raise ValueError(f"Input DataFrame must contain columns: {required_columns}")

                # Compute the count of distinct two-wheelers (motorcycles)
                count_2_wheelers = (
                    df.select("VIN")
                    .filter(col("VEH_BODY_STYL_ID").like("%MOTORCYCLE%"))
                    .agg(countDistinct("VIN").alias("count"))
                )

                return count_2_wheelers

            except ValueError as ve:
                print(f"ValueError: {ve}")
                raise

            except Exception as exception:
                print(f"An error occurred: {exception}")
                raise


    def top_5_car_brands_airbags_not_deployed(self,df_1,df_2):
        """
        Identifies the top 5 car brands where airbags were not deployed during crashes 
        that resulted in the death of a driver.

        Parameters:
        ----------
        df_primary_person : pyspark.sql.DataFrame
            Input DataFrame containing primary person crash details, including columns
            `PRSN_TYPE_ID`, `PRSN_INJRY_SEV_ID`, `PRSN_AIRBAG_ID`, and `CRASH_ID`.
        
        df_units : pyspark.sql.DataFrame
            Input DataFrame containing vehicle details, including columns `CRASH_ID`, 
            `VEH_MAKE_ID`, and `DEATH_CNT`.

        Returns:
        -------
        pyspark.sql.DataFrame
            A DataFrame with two columns: `VEH_MAKE_ID` and `count`, representing the top 5 
            vehicle makes with the highest number of such incidents.
        """
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
        """
        Computes the count of hit-and-run incidents where the driver had a valid license.

        Parameters:
        ----------
        df_primary_person : pyspark.sql.DataFrame
            Input DataFrame containing primary person crash details, including columns
            `DRVR_LIC_CLS_ID` and `CRASH_ID`.

        df_units : pyspark.sql.DataFrame
            Input DataFrame containing vehicle details, including columns `CRASH_ID`
            and `VEH_HNR_FL`.

        Returns:
        -------
        pyspark.sql.DataFrame
            A DataFrame containing a single row and column (`count`) representing the count 
            of hit-and-run incidents with valid license holders.
        """

        df_primary_person = df_1
        df_units          = df_2

        try:
            df_primary_person = df_primary_person.dropna(subset=["DRVR_LIC_CLS_ID"])
            exclude_list = ['OTHER/OUT OF STATE','UNKNOWN','UNLICENSED']
            df_primary_person_valid_license = df_primary_person.filter(~col('DRVR_LIC_CLS_ID').isin(exclude_list))
            df_units_hnr = df_units.filter(col('VEH_HNR_FL')== 'Y')
            df_hnr_crash_id  = df_units_hnr.select('CRASH_ID').dropDuplicates(subset=["CRASH_ID"])
            df_valid_lic_hnr= df_primary_person_valid_license.join(df_hnr_crash_id,df_primary_person_valid_license.CRASH_ID==df_hnr_crash_id.CRASH_ID,how='leftsemi')
            row_count =  df_valid_lic_hnr.agg(count('CRASH_ID').alias('count'))
            
        
        except Exception as exception:
            print('Error::{}'.format(exception)+"\n")
        finally:
            return row_count

    def noFemale_invoved(self,df_1):
        """
        Identifies the state with the highest number of incidents where no females 
        were involved, based on driver license state.

        Parameters:
        ----------
        df_primary_person : pyspark.sql.DataFrame
            Input DataFrame containing primary person crash details, including columns
            `PRSN_GNDR_ID` and `DRVR_LIC_STATE_ID`.

        Returns:
        -------
        pyspark.sql.DataFrame
            A DataFrame with one row containing the state (`DRVR_LIC_STATE_ID`) with 
            the highest count of such incidents and the corresponding count.
        """
        df_primary_person = df_1

        try:
            df_females_not_involed = df_primary_person.filter(col('PRSN_GNDR_ID') != "FEMALE").groupBy("DRVR_LIC_STATE_ID").agg(count("*").alias("count")).orderBy(desc("count")).limit(1)
        except Exception as exception:
            print('Error::{}'.format(exception)+"\n")
        finally:
            return df_females_not_involed

    def states_with_highest_injuries(self,df_1):
        """
        Identifies the vehicle makes with the highest total injury counts, ranking them, 
        and filtering to include only ranks between 3 and 5.

        Parameters:
        ----------
        df_units : pyspark.sql.DataFrame
            Input DataFrame containing vehicle information, including columns `VEH_MAKE_ID`
            and `TOT_INJRY_CNT`.

        Returns:
        -------
        pyspark.sql.DataFrame
            A DataFrame containing the vehicle makes ranked 3rd and above based on their 
            total injury count, with columns `VEH_MAKE_ID`, `Total_injury_count`, and `rank`.

        """
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
        """
        Determines the top ethnic group associated with each vehicle body style based 
        on the count of occurrences.

        Parameters:
        ----------
        df_primary_person : pyspark.sql.DataFrame
            Input DataFrame containing primary person information, including columns
            `CRASH_ID`, `UNIT_NBR`, and `PRSN_ETHNICITY_ID`.

        df_units : pyspark.sql.DataFrame
            Input DataFrame containing vehicle information, including columns `CRASH_ID`,
            `UNIT_NBR`, and `VEH_BODY_STYL_ID`.

        Returns:
        -------
        pyspark.sql.DataFrame
            A DataFrame containing the top ethnic group for each vehicle body style, with 
            columns `VEH_BODY_STYL_ID`, `PRSN_ETHNICITY_ID`, and the count of occurrences.
        """
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
        """
        Identifies the top 5 ZIP codes with the highest number of crashes involving 
        alcohol-influenced drivers.

        Parameters:
        ----------
        df_primary_person : pyspark.sql.DataFrame
            Input DataFrame containing primary person crash details, including columns
            `PRSN_ALC_RSLT_ID` and `DRVR_ZIP`.

        Returns:
        -------
        pyspark.sql.DataFrame
            A DataFrame containing the top 5 ZIP codes with alcohol-influenced crashes, 
            including columns `DRVR_ZIP` and `Crashes`.
        """
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
        """
        Identifies the number of incidents where cars with no insurance and no reported damages
        have sustained significant property damage Damage Level (VEH_DMAG_SCL~)  > 4.

        Parameters:
        ----------
        df_charges : pyspark.sql.DataFrame
            Input DataFrame containing charge details, including columns `CHARGE`, `CRASH_ID`, and `UNIT_NBR`.
        
        df_damages : pyspark.sql.DataFrame
            Input DataFrame containing damage details, including `CRASH_ID`.

        df_units : pyspark.sql.DataFrame
            Input DataFrame containing unit details, including `CRASH_ID`, `UNIT_NBR`, `VEH_DMAG_SCL_1_ID`, and `VEH_DMAG_SCL_2_ID`.
        """
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
                             .select('CRASH_ID').distinct()
            result = result.agg(count('CRASH_ID').alias('count'))
            print(result)
        except Exception as exception:
            print('Error::{}'.format(exception)+"\n")
        finally:
            return result


    def top_vehicle_makers(self,df_1,df_2,df_3):
        """
        Identifies the top 5 vehicle makes involved in speed-related charges, 
        based on various filtering criteria including vehicle color, license state, 
        and driver license status.

        Parameters:
        ----------
        df_charges : pyspark.sql.DataFrame
            Input DataFrame containing charge details, including columns `CHARGE`, `CRASH_ID`, and `UNIT_NBR`.
        
        df_units : pyspark.sql.DataFrame
            Input DataFrame containing unit details, including columns `VEH_COLOR_ID`, `VEH_LIC_STATE_ID`, `VEH_MAKE_ID`, and `UNIT_NBR`.
        
        df_primary_person : pyspark.sql.DataFrame
            Input DataFrame containing primary person details, including columns `DRVR_LIC_CLS_ID`, `CRASH_ID`, and `UNIT_NBR`.
        """
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