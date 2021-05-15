# DSC202FinalProject

The project consists of 4 notebooks that can be run in a Databricks Premium account from 1 main notebook "DSCC202-402 Forecasting Flight Delay Final Project".
The 4 notebooks each consist of one separate part of building a full data intensive application. Our input is two bronze data tables (flight data and weather data).
These are loaded via streaming, transformed via data analysis and joined into a silver table from which we create machine learning models. These models are then
tracked via MLflow. For each model we have a production version and a staging version that is yet to be tested. The monitoring of the staging version when compared
to the production version can be done in Model Monitoring via data visualization.

1. Extract Transform and Load (ETL)
	* Bronze data sources of airline data and weather data are already in the dscc202_db.
	* Document your pipeline according to the object and class notation that we used in the Moovio example.
	* Feature engineering should be well documented.  E.g. what transformations are being employed to form the Silver data from the Bronze data.
	* Schema validation and migration should be included in the Bronze to Silver transformation.
	* Optimization with partitioning and Z-ordering should be appropriately employed.
	* Streaming construct should be employed so any data added to Bronze data sources will be automatically ingested by your application by running the ELT code.
	* ELT code should be idempotent.  No adverse effects for multiple runs.
2. Exploratory Data Analysis (EDA)
  * Findings and filtering of Bronze/Silver data.
3. MLops Lifecycle
  * Use the training start and end date widgets to specify the date range to be used in the training and test of a given model.
  * Training model(s) at scale to estimate the arrival or departure time of a flight before it leaves the gate.
  * Register training and test data versions as well as parameters and metrics using mlflow
  * Including model signature in the published model
  * Hyperparameter tuning at scale with mlflow comparison of performance
  * Orchestrating workflow staging to production using clear test methods
  * Parameterize the date range that a given model is covering in its training set.
4. Model Monitoring
	* Use the training and inference date widgets to highlight model performance on unseen data
	* Specify your criteria for retraining and promotion to production.
	* Use common model performance visualizations to highlight the performance of Staged Model vs. Production Model. E.g. residual plot comparisons
	* Include code that allows the monitoring notebook to “promote” a model from staging to production.

### Instructions
* The main notebook contains widgets that can be set and used for each of the subsequent notebooks.
* The Model Monitoring notebook contains a separate widget "Promote?" that can be used to promote a staging model to a production one after having observed
its performance compared to the model currently in production.

![alt text](https://data-science-at-scale.s3.amazonaws.com/images/DIA+Framework-DIA+Process+-+1.png)
