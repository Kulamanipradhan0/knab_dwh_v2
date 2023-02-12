cd ~
mkdir knab_dwh

cd knab_dwh


sudo apt-get install docker
sudo apt-get install wget
sudo apt-get install unzip

rm -r knab_dwh-main
wget https://github.com/Kulamanipradhan0/knab_dwh/archive/refs/heads/main.zip
unzip main.zip

cd knab_dwh-main

docker build -t knab_dwh_pg -f Dockerfile_pg .
docker run -id --rm --name knab_dwh_pg -p 9000:5432 knab_dwh_pg


docker build -t knab_dwh_airflow -f Dockerfile_airflow .
docker run -i --rm -d --network="host" -p 8080:8080 --name knab_dwh_airflow -v ~/knab_dwh/knab_dwh/etl_repo/report/:/code_base/etl_repo/report/  knab_dwh_airflow



