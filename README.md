
# CASE STUDY
## CAR Crash Analysis

### Objective
Develop a modular spark application spark-submit to provide results for given tasks.

#### Requirements
Application should perform below analysis and store the results for each analysis.
1. Analytics 1: Find the number of crashes (accidents) in which number of males killed are greater than 2? 
2.	Analysis 2: How many two wheelers are booked for crashes? 
3.	Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
4.	Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run? 
5.	Analysis 5: Which state has highest number of accidents in which females are not involved? 
6.	Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
7.	Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
8.	Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
9.	Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
10.	Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)

#### Expected Output:
*       Develop an application which is modular & follows software engineering best practices (e.g. Classes, docstrings, functions config driven, command line executable through spark-submit)
*     Code should be properly organized in folders as a project.
*     Input data sources and output should be config driven
*     Code should be strictly developed using Dataframe APIs (Do not use Spark SQL)
*     Upload the entire project in Github repo


Run the application by using following command :-
```
spark-submit --master local[*] main.py
```
main.py is the main file invokes the JobBuilder class to build and execute analysis based on question_id mentioned in config.json file.

You can find the rough version of the analysis in the test_notebook.ipynb file in the notebook directory.

## ⛏️ Built Using <a name = "built_using"></a>

- [Pyspark](https://spark.apache.org/docs/latest/api/python/) - Data Processing Framework
- [Jupyter Notebook](https://jupyter.org/) - Data Analysis Tool

