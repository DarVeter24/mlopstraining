
https://airflow.apache.org/docs/helm-chart/stable/index.html

https://github.com/apache/airflow/blob/main/chart/values.yaml

# 1. Сборка образа
docker build -t my-airflow-mlops:latest .

# 2. Проверить, что образ успешно собран и посмотреть список тегов
docker images | grep my-airflow-mlops

# 3. Посмотреть историю слоёв образа (проверить, на каком базируется)
docker history my-airflow-mlops:latest

# 4. Посмотреть установленную версию Airflow и Python внутри образа
docker run --rm my-airflow-mlops:latest airflow version
docker run --rm my-airflow-mlops:latest python --version

# 5. Проверить, какие пакеты установлены (например, pyspark, mlflow)
docker run --rm my-airflow-mlops:latest pip show pyspark
docker run --rm my-airflow-mlops:latest pip show mlflow

# 6. Тегирование для Docker Hub
docker tag my-airflow-mlops:latest mrhagal/my-airflow-mlops:v1

# 7. Логин в Docker Hub
docker login

# 8. Пуш в Docker Hub
docker push mrhagal/my-airflow-mlops:v1

# 9. Проверка загруженного образа
docker pull mrhagal/my-airflow-mlops:latest

# 10. Проверить наличие образа локально и с каким тегом
docker images | grep mrhagal/my-airflow-mlops

# 11. (опционально) Зайти внутрь контейнера для ручной проверки:
docker run -it --rm my-airflow-mlops:latest bash
# внутри bash можно сделать: pip list, ls /opt/airflow, env | grep JAVA_HOME и др.
