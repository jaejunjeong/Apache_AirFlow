#airflow 시작
Start_airflow

# DAG 리스트 확인
airflow dags list

# 특정 DAG 확인
airflow dags list | grep "DAG이름"

#DAG의 task 확인
airflow tasks list DAG : 

#DAG 실행
airflow dags unpause DAG

#DAG 중지
airflow dags pause DAG
