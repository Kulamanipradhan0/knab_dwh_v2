#########################################################################################################
##### This Document describes how to set up all required softwares related to KNAB DWH assignment   #####
#########################################################################################################
#####                           System Specification                                                #####    
##### Ubuntu 18                                                                                     #####   
##### Java 1.8 jdk                                                                                  #####   
##### Python 3.7                                                                                    #####    
##### Postgres 11.13 (Port : 9000)    					               	    	    #####   
##### Apache Airflow 2.1.2    (Port : 8080)                                                         #####    
#########################################################################################################

NOTE : This has been tested with docker in linux environment.
Assumption : We are in home directory.
cd ~

#########################################################################################################
##### Automatic Environment Set up using Docker and start_up script                                 #####
#########################################################################################################

1. Download the complete codebase repository from git hub.
Download the start_up.sh file from git hub.
wget https://github.com/Kulamanipradhan0/knab_dwh/blob/main/start_up.sh

chmod 775 start_up.sh
sh start_up.sh

1. Test if Airflow is running : http://localhost:8080/

2. Connect to Postgres using 
psql -h localhost -p 9000 -U postgres -d postgres
Password for above user : password

3. Refresh Airflow dag screen to see knab_dwh dag. Activate it and trigger it. It should load the data 
for one business date(2020-12-31)





----------------------------------------------------------------------------------------------------------



Below describes Manual Installation of Postgres & Airflow



#########################################################################################################
##### Java 8 jdk      Installation PG11.13                                                          #####
#########################################################################################################

1. Update the ubuntu repository.
sudo apt-get update

2. Install Java 8 jdk.
sudo apt-get install openjdk-8-jdk

3. Test it.
java -version


#########################################################################################################
##### Python 3.7 Installation                                                                       #####
#########################################################################################################

1. Update the ubuntu repository.
sudo apt-get update

2. Install Python 3.7 .
sudo apt install python3.7

3. Test it.
python3 --version


#########################################################################################################
##### Postgres 11.13 Installation                                                                    #####
#########################################################################################################

1. Update the ubuntu repository.
sudo apt update && sudo apt -y upgrade

2. Install wget and vim to download a package.
sudo apt install -y wget vim

3. Add Postgres repository keys into local.
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
RELEASE=$(lsb_release -cs)
echo "deb http://apt.postgresql.org/pub/repos/apt/ ${RELEASE}"-pgdg main | sudo tee  /etc/apt/sources.list.d/pgdg.list
cat /etc/apt/sources.list.d/pgdg.list

4. Install Postgres 11.13 version.
sudo apt -y install postgresql-11 or sudo apt -y install postgresql-11.13

5. Lets test if postgres Installation.
sudo su - postgres
psql -c "alter user postgres with password 'SuperUserPassword'"


#########################################################################################################
##### Apache Airflow Installation                                                                   #####
#########################################################################################################


1. Create a airflow directory, where we will place all our dags and code base
mkdir ~/Assignment_KP/tools/airflow
cd ~/Assignment_KP/tools/airflow

2. Lets install virtualenv utility.
sudo pip install virtualenv

3. Lets create a virtual environment for airflow.
sudo virtualenv airflow-venv

4. Lets active our new virtualenv for airfow.
source airflow-venv/bin/active

5. Lets declare our AIRFLOW_HOME path.
export AIRFLOW_HOME=~/Assignment_KP/tools/airflow/airflow-venv

6. Lets Install airflow in this virtualenv. Follow this url to see the steps.
https://airflow.apache.org/docs/apache-airflow/stable/installation.html

AIRFLOW_VERSION=2.1.2
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
# For example: 3.6
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-no-providers-${PYTHON_VERSION}.txt"
# For example: https://raw.githubusercontent.com/apache/airflow/constraints-no-providers-2.1.2/constraints-3.6.txt
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

7. Lets Initialize the DB for airflow.
airflow db init

8. Lets Create an admin user with password as admin.
airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin

9. Create a dags directory. Where we will keep all our codebase.
mkdir dags

10. Lets change airflow.conf configuration file for pointing to this newly created dags directory
Variable name to change : dags_folder 
Replace the line with :
dags_folder = ~/Assignment_KP/tools/airflow/dags

11. Lets install all dependency python packages, which is used in airflow for Job execution.
pip install psycopg2
pip install psycopg2-binary
pip install pandas
pip install xlsxwriter
pip install apache-airflow['cncf.kubernetes']


12. Let's test the apache webserver. 
airflow webserver --port 8080
URL : http://localhost:8080/

13. Let's test the apache scheduler in another terminal.
airflow scheduler

14. Keep it running



#########################################################################################################
#####                                           END                                                 #####
#########################################################################################################
