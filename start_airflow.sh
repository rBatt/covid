sudo service postgresql start
sudo service rabbitmq-server start
airflow initdb
airflow scheduler -D &
airflow worker -D &
airflow webserver -p 8080 && fg