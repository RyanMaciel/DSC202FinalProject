# Databricks notebook source
# MAGIC %md
# MAGIC ## Description
# MAGIC In project you and your group will be developing an end to end Data Intensive Application (DIA) that predicts flight delays using 6 years of US flight recording data along with  hourly weather reports.
# MAGIC - <a href='https://transtats.bts.gov/Fields.asp?gnoyr_VQ=FGJ&flf_gnoyr_anzr=g_bagVZR_eRcbegVaT&h5r4_gnoyr_anzr=er2146v0t%20Pn44vr4%20b0-gvzr%20cr4s14zn0pr%20(EMLK-24r5r06)&lrn4_V0s1=E&Sv456_lrn4=EMLK&Yn56_lrn4=FDFE&en6r_V0s1=D&S4r37r0pB=Z106uyB&Qn6n_S4r37r0pB=N007ny,d7n46r4yB,Z106uyB'>Flight Delay Data Description</a>
# MAGIC - [Hourly weather reports Data Description](https://www.ncei.noaa.gov/data/global-hourly/doc/isd-format-document.pdf)
# MAGIC 
# MAGIC Consider this illustration of the different activities that factor into the total amount of time required for a flight and whether a given flight adheres to its scheduled departure and arrival times as well as the familiar process and tech stack that we have covered in this course.
# MAGIC <table border=0>
# MAGIC   <tr><td><h2>Application</h2></td><td><h2>Process</h2></td></tr>
# MAGIC   <tr><td>![Image](https://data-science-at-scale.s3.amazonaws.com/images/Flight+Time+Illustration.png)</td><td><img src='https://data-science-at-scale.s3.amazonaws.com/images/DIA+Framework-DIA+Process+-+1.png' width=680></td></tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Resources and Naming Conventions
# MAGIC 
# MAGIC - Each group has been assigned a specific Databricks Spark Cluster named **dscc202-groupxx-cluster** (all provisioned the same) with 1 driver node and up to 8 workers.
# MAGIC - Each group has a specific AWS S3 bucket they will mount to store their project tables and model artifacts.  The project starting archive has code to do this. **/mnt/dscc202-groupxx-datasets**
# MAGIC - Each group has a pre-provisioned database **dscc202_groupxx_db** that they should use for all of their hive metastore tables.
# MAGIC - Each group should create a specific model in the mlflow registry **groupxx_model** that they should use for their project.
# MAGIC - Each group should create a specific MLflow experiment for all of their project runs and the model artifacts should be stored in their group specific bucket as well. **dscc202_groupxx_experiment**
# MAGIC 
# MAGIC ![Image](https://data-science-at-scale.s3.amazonaws.com/images/Flight+Project+Resources.png)
# MAGIC 
# MAGIC **IMPORTANT**: See the configuration notebook under **includes** to set your group designation

# COMMAND ----------

# DBTITLE 0,Project Structure
# MAGIC %md
# MAGIC ## Project Structure
# MAGIC Each group is expected to divide their work among a set of notebooks within the Databricks workspace.  A group specific project archive should be setup in github and each group member can work on their specific branch of that repository and then explicitly merge their work into the “master” project branch when appropriate. (see the class notes on how to do this).  The following illustration highlights the recommended project structure.  This approach should make it fairly straight forward to coordinate group participation and work on independent pieces of the project while having a well identified way of integrating those pieces into a whole application that meets the requirements specified for the project.
# MAGIC 
# MAGIC ![Image](https://data-science-at-scale.s3.amazonaws.com/images/Flight+Project+Structure.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grading
# MAGIC **Project is Due no later than May 14th 2021**
# MAGIC <p>Each student in a given group should participate in the design and development of the application.  
# MAGIC The group should coordinate and divide up the responsibilities needed to complete the project.  
# MAGIC Each student in a given group will provide a **peer assessment** of their teammates submitted via blackboard (see the link below).<br>
# MAGIC This will be a two part assessment:  one for participation and the other for contribution.  
# MAGIC Please rate each member as satisfactory, unsatisfactory or exceptional on each dimension.<br> 
# MAGIC In addition, each student will be required to turn in a written description via blackboard of how <br>
# MAGIC DIA challenges are being met by the systems and practices that we studied in this course while <br>
# MAGIC using their project as a supporting example. (see link below for the questions)</p>
# MAGIC 
# MAGIC #### Points Allocation
# MAGIC - Individual - written description and peer assessment - 15 pts
# MAGIC - Group - Extract Tansform and Load (ETL) - 5 points
# MAGIC - Group - Exploratory Data Analysis (EDA) - 5 points
# MAGIC - Group - Modeling - 10 points
# MAGIC - Group - Monitoring - 5 points
# MAGIC - Group - Application - 10 points
# MAGIC 
# MAGIC Total of 50 points.  Good luck and have fun!
# MAGIC 
# MAGIC [Fill out and upload the following to Blackboard](https://data-science-at-scale.s3.amazonaws.com/images/Final+Project+Description+-+DSCC202-402+Data+Science+at+Scale+-+V2.pdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Flight Delay Application
# MAGIC ![Image](https://data-science-at-scale.s3.amazonaws.com/images/Flight+Application+v2.png)
# MAGIC 
# MAGIC Your application will focus on 13 of the busiest airports in the US.  In particular, 
# MAGIC - JFK - INTERNATIONAL AIRPORT, NY US
# MAGIC - SEA - SEATTLE TACOMA INTERNATIONAL AIRPORT, WA US
# MAGIC - BOS - BOSTON, MA US
# MAGIC - ATL - ATLANTA HARTSFIELD INTERNATIONAL AIRPORT, GA US
# MAGIC - LAX - LOS ANGELES INTERNATIONAL AIRPORT, CA US
# MAGIC - SFO - SAN FRANCISCO INTERNATIONAL AIRPORT, CA US
# MAGIC - DEN - DENVER INTERNATIONAL AIRPORT, CO US
# MAGIC - DFW - DALLAS FORT WORTH AIRPORT, TX US
# MAGIC - ORD - CHICAGO O’HARE INTERNATIONAL AIRPORT, IL US
# MAGIC - CVG - CINCINNATI NORTHERN KENTUCKY INTERNATIONAL AIRPORT, KY US'
# MAGIC - CLT - CHARLOTTE DOUGLAS AIRPORT, NC US
# MAGIC - DCA - WASHINGTON, DC US
# MAGIC - IAH - HOUSTON INTERCONTINENTAL AIRPORT, TX US
# MAGIC 
# MAGIC Your application should allow the airport and time mark to be chosen using user interface widgets.  The time mark for predictions should of course be restricted to a period of time after the training data that was used to build the model. The application should form an estimate for each flight that is due to arrive or depart the specified airport over the next 24 hours after the time mark.  It is important to note that some flights that are inbound will have already left their originating airport at the time mark.  In these cases, the application should form an estimate that is focused on “flight duration” which is likely different than estimating delay when the period of interest includes time before the airplane has departed the originating airport.

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ### Application Widgets
# MAGIC - Airport Code - dropdown list to select the airport of interest.
# MAGIC - Training Start Date - when the training should start (note 6 years of data in the archive)
# MAGIC - Training End Date - when the training should end
# MAGIC - Inference Date - this will be set to 1 day after the training end date although any date after training can be entered.

# COMMAND ----------

from datetime import datetime as dt
from datetime import timedelta
dbutils.widgets.removeAll()

dbutils.widgets.dropdown("Airport Code", "JFK", ["JFK","SEA","BOS","ATL","LAX","SFO","DEN","DFW","ORD","CVG","CLT","DCA","IAH"])
dbutils.widgets.text('Training Start Date', "2018-01-01")
dbutils.widgets.text('Training End Date', "2018-02-01")
dbutils.widgets.text('Inference Date', (dt.strptime(str(dbutils.widgets.get('Training End Date')), "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d"))

training_start_date = str(dbutils.widgets.get('Training Start Date'))
training_end_date = str(dbutils.widgets.get('Training End Date'))
inference_date = str(dbutils.widgets.get('Inference Date'))
airport_code = str(dbutils.widgets.get('Airport Code'))
print(airport_code,training_start_date,training_end_date,inference_date)

# COMMAND ----------

# run link to the application notebook
#status = dbutils.notebook.run(<PATH TO YOUR APPLICATION NOTEBOOK>, 3600, "00.Airport_Code":airport_code,"01.training_start_date":training_start_date,"02.training_end_date":training_end_date,"03.inference_date":inference_date})
# if status == "Success" print("Passed") else print("Failed")
# NOTE NOTEBOOK SHOULD RETURN dbutils.notebook.exit("Success") WHEN IT PASSES


# COMMAND ----------

# MAGIC %md
# MAGIC ### Model Monitoring
# MAGIC - Use the training and inference date widgets to highlight model performance on unseen data
# MAGIC - Specify your criteria for retraining and promotion to production.
# MAGIC - Use common model performance visualizations to highlight the performance of Staged Model vs. Production Model. E.g. residual plot comparisons
# MAGIC - Include code that allows the monitoring notebook to “promote” a model from staging to production.

# COMMAND ----------

# run link to the monitoring notebook
status = dbutils.notebook.run("Model_Monitoring", 3600, {"Airport Code": airport_code, "Training Start Date": training_start_date, "Training End Date": training_end_date, "Inference Date": inference_date})
if status == "Success":
  print("Passed") 
else:
  print("Failed")
# NOTE NOTEBOOK SHOULD RETURN dbutils.notebook.exit("Success") WHEN IT PASSES

# COMMAND ----------

# MAGIC %md
# MAGIC ### MLops Lifecycle
# MAGIC - Use the training start and end date widgets to specify the date range to be used in the training and test of a given model.
# MAGIC - Training model(s) at scale to estimate the arrival or departure time of a flight before it leaves the gate.
# MAGIC - Register training and test data versions as well as parameters and metrics using mlflow
# MAGIC - Including model signature in the published model
# MAGIC - Hyperparameter tuning at scale with mlflow comparison of performance
# MAGIC - Orchestrating workflow staging to production using clear test methods
# MAGIC - Parameterize the date range that a given model is covering in its training set.

# COMMAND ----------

# run link to the modeling notebook
#status = dbutils.notebook.run(<PATH TO YOUR ML MODELING NOTEBOOK>, 3600, "00.Airport_Code":airport_code,"01.training_start_date":training_start_date,"02.training_end_date":training_end_date,"03.inference_date":inference_date})
# if status == "Success" print("Passed") else print("Failed")
# NOTE NOTEBOOK SHOULD RETURN dbutils.notebook.exit("Success") WHEN IT PASSES

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extract Transform and Load (ETL)
# MAGIC - Bronze data sources of airline data and weather data are already in the dscc202_db.
# MAGIC - Document your pipeline according to the object and class notation that we used in the Moovio example.
# MAGIC - Feature engineering should be well documented.  E.g. what transformations are being employed to form the Silver data from the Bronze data.
# MAGIC - Schema validation and migration should be included in the Bronze to Silver transformation.
# MAGIC - Optimization with partitioning and Z-ordering should be appropriately employed.
# MAGIC - Streaming construct should be employed so any data added to Bronze data sources will be automatically ingested by your application by running the ELT code.
# MAGIC - ELT code should be idempotent.  No adverse effects for multiple runs.

# COMMAND ----------

# run link to the modeling notebook
#status = dbutils.notebook.run(<PATH TO YOUR ETL NOTEBOOK>, 3600, "00.Airport_Code":airport_code,"01.training_start_date":training_start_date,"02.training_end_date":training_end_date,"03.inference_date":inference_date})
# if status == "Success" print("Passed") else print("Failed")
# NOTE NOTEBOOK SHOULD RETURN dbutils.notebook.exit("Success") WHEN IT PASSES

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploratory Data Analysis (EDA)
# MAGIC - Follow the guidelines in [Practical Advice for the analysis of large data](https://www.unofficialgoogledatascience.com/2016/10/practical-advice-for-analysis-of-large.html)
# MAGIC - Clear communication of findings and filtering of Bronze/Silver data.
# MAGIC - Pandas Profiling and Tensorflow Data Validation libraries can be very helpful here... hint!

# COMMAND ----------

#"00.Airport_Code":airport_code,"01.training_start_date":training_start_date,"02.training_end_date":training_end_date,"03.inference_date":inference_date
# run link to the EDA notebook
status = dbutils.notebook.run("./EDA", 3600, {})
if status == "Success":
   print("Passed")
else:
  print("Failed")
# NOTE NOTEBOOK SHOULD RETURN dbutils.notebook.exit("Success") WHEN IT PASSES

# COMMAND ----------

