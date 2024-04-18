# AquaTech User Activity Analysis
Introduction
In the realm of aquatic-focused startups, understanding user behavior is paramount. In pursuit of this understanding, AquaTech has embarked on an analysis of user activity on its platform. Data pertaining to user activity is collected and temporarily stored in the internal storage platform, residing on user devices. Subsequently, this data is pulled by a tool every minute for all users. The pulled data is stored in JSON format with a timestamp naming convention, such as 20240402-113713.json, signifying the data pull time as April 2, 2024, at 11:37:13 AM.
Cloning the Repository
To get started, clone this repository:
bashCopy code
git clone xxx
cd xxx xxx

Project Overview
In this project, JSON files have been stored in the raw_data.zip file (located on Google Drive) with default folder and file names for the months of April and May 2024. To download the file, execute the bash script ./download_raw_data.sh, and the output will be saved in the ./raw_data.zip file.
If the current datetime falls within April or May 2024, simply unzip the raw_data.zip file and save it to the ./airflow/data/ folder:
bashCopy code
unzip ./raw_data.zip -d ./airflow/data/ 
However, if the current datetime is outside of April or May 2024, execute the bash script rename.sh $1 $2 $3, where parameter 1 represents the current year, parameter 2 represents the current month, and parameter 3 represents the next month (e.g., bash ./rename.sh 2024 04 05). Note that executing the rename.sh script can be time-consuming. The output of the rename.sh execution will be saved in the ./airflow/data/raw_data folder with folder and file structures corresponding to the current datetime. 
Execute the tree ./airflow/data/raw_data command to view the folder structure.
Example File Structure:
./airflow/data/raw_data
├── 2024-04-01
│  ├── 20240401-000000.json
│  ├── 20240401-000001.json
│  ├── 20240401-000010.json
│  ├── 20240401-000017.json

Problem Statement
Although the filenames are created in the format of the current datetime (to match the data_interval_start parameter in Airflow), the actual raw data is for event time in 2022 (timestamp = 2022). Below is an example JSON record from the raw data:
[{
  "distinct_id": "21920",
  "event": "$identify",
  "timestamp": "2022-10-01T14:44:35.546Z",
  "uuid": "01839401-d757-0001-295b-23cf4fc0a94a",
  "elements": [],
  "$geoip_city_name": "Palembang",
  "$geoip_country_name": "Indonesia",
  "$geoip_country_code": "ID",
  "$geoip_continent_name": "Asia",
  "$geoip_continent_code": "AS",
  "$geoip_latitude": -2.9146,
  "$geoip_longitude": 104.7535,
  "$geoip_time_zone": "Asia/Jakarta",
  "$geoip_subdivision_1_code": "SS",
  "$geoip_subdivision_1_name": "South Sumatra"
}]
We assume to process the data for the current datetime (processing datetime = current datetime), but the events themselves occurred in 2022 (event_time = 2022).
The interesting thing here is that records with past event times may appear in the new JSON files (e.g., a JSON record with "timestamp": "2022-09-22T11:09:43.929Z" appears in the file 20240413-030129.json). This is a kind of "late arriving data". These delayed data will affect the daily active user (DAU) and monthly active user (MAU) tables. Therefore, if there are late-arriving records, the DAU for the related day (event_time) will be recalculated, as will the MAU for the related month.
Output
Tables:
•	event_data: Contains parsed data from JSON files plus a dl_updated_at column indicating the timestamp when records were processed (processing time).
•	dau (daily active user): Contains a summary of the number of active users in daily units (based on event time).
•	mau (monthly active user): Contains a summary of the number of active users in monthly units (based on event time).
Dashboard (Looker - optional)
Data Tools Used
•	Google Cloud Platform (Cloud Storage and BigQuery)
•	Terraform
•	Docker
•	DBT (Data Build Tool)
•	Airflow
•	Looker (optional)

Google Cloud Preparation
•	Create a GCP account.
•	Create a service account with appropriate access.
•	Download the service account JSON file and save it in the gcp_platform folder (rename the file to service-account.json).
Terraform (for activating Cloud Storage and BigQuery services)
•	Install Terraform on your machine/VM.
•	Change directory to the gcp_platform folder.
•	In the gcp_platform/variables.tf file, update the Terraform variables for project, region, and location according to your preferences.
•	Execute terraform init and terraform apply to create the Cloud Storage bucket and BigQuery dataset.
•	Open google cloud consoles, and navigate to bigquery. Execute the query for the SQL commands in the ./ddl_table.sql file.
DBT
•	Update sources.database in the ./de-project/airflow/data/dbt/user_activity/models/staging/schema.yml file according to your project Id.
•	We will use the materialize = incremental configuration for the DAU and MAU tables to handle "late arriving data".
Airflow
1. Preparation
•	Airflow will be deployed using Docker, so make sure Docker is installed on your machine/VM.
•	Copy the service-account.json file to the ./airflow/data folder (to be used in creating a Google Cloud connection in Airflow).
•	Navigate to the ./airflow directory and execute docker-compose up -d.
•	Ensure all services are up and running (docker ps).
•	DBT commands will be executed via Airflow, so we need to install dbt-bigquery and astronomer-cosmos in the airflow worker and scheduler containers:
bashCopy code
docker exec airflow-airflow-worker-1 bash -c "pip install astronomer-cosmos dbt-bigquery"  &&
docker exec airflow-airflow-scheduler-1 bash -c "pip install astronomer-cosmos dbt-bigquery" && docker restart airflow-airflow-worker-1

•	Log in to the Airflow web server UI (http://localhost:8080) with the username airflow and password airflow. Hover over the admin tab and click connections.
•	Create a new connection with Connection Type = Google Cloud and Connection Id = ‘google_client’. Fill in the Project Id according to your project Id, and fill in the Keyfile Path referring to service-account.json (/opt/airflow/data/service-account.json).
•	Click the test button at the bottom to test the connection to Google Cloud with the predefined configuration. A successful connection test will display "Connection successfully tested" at the top of the web page (scroll up), and then save the connection.
2. DAGs
In this project, we run two DAGs: get_data and event_data_transformations.

DAG get_data:
This DAG runs every hour (schedule = hourly) and retrieves and processes raw data JSON files according to the execution time parameter in Airflow (not all JSON files are processed at once). The output of one run of the DAG is a CSV file uploaded to Cloud Storage with the naming format: output_{data_interval_start}_{data_interval_end}.csv (e.g., output_20240413030000_20240413040000.csv is the file generated when the DAG runs for the schedule interval from April 13, 2024, at 03:00:00 to April 13, 2024, at 04:00:00). Therefore, in one day, 24 files will be generated. 
When the data_interval_start is at 00:00 early in the morning, this DAG will trigger the event_data_transformations DAG for execution. 

Activate the DAG by clicking on the DAG tab on the web and unpausing the get_data DAG, then the job to extract data from the JSON file will run and store the results in Cloud Storage.

DAG event_data_transformations:
Unpause the event_data_transformations DAG. This DAG runs using the data aware scheduling (dataset schedule) triggered by the get_data DAG. When running, this DAG will execute a BigQuery query to create an external table from the CSV file in Cloud Storage for a one-day range and then insert it into the event_data table. Next, Airflow will execute the DBT command to transform event_data table to upsert the DAU and MAU tables.

Looker (optional)
Create visualizations according to your preferences. Here we create visualizations using Looker with the DAU and MAU datasources. Below is example of dashboard created using looker studio
![Example Image](https://github.com/maulanaady/AquaTech-User-Activity-Analysis/blob/main/images/dashboard.png)
