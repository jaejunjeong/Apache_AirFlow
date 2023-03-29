 #move dag to AIRFLOW dags directory
 cp my_first_dag.py $AIRFLOW_HOME/dags
 #check dag moved
 airflow dags list|grep "my-first-dag"
 #check dag's task
 airflow tasks list my-first-dag
