# Регулярное переобучение модели обнаружения мошенничества

**Домашнее задание №6**  
**Курс MLOps**  
**Образовательная платформа «Otus»**

## Цель работы

В данном домашнем задании потренировался в использовании PySpark для обучения модели, настройке MLFlow с удаленным сервером отслеживания и хранилищем артефактов, сохранении всех артефактов, включая модель, в MLFlow с использованием Object Storage и настройке переобучения модели по расписанию с помощью Airflow на новых данных.

## ⚡ Особенность реализации

**Вместо облачной среды Yandex Cloud использовалось bare metal решение на базе Proxmox:**
- **Гипервизор:** Proxmox VE для управления виртуальными машинами
- **Kubernetes кластер** развернут на ВМ в Proxmox на физических серверах
- **MinIO** как S3-совместимое хранилище развернуто локально в кластере
- **PostgreSQL** для MLflow развернута в кластере
- Все компоненты работают на собственной инфраструктуре без использования управляемых облачных сервисов

## Выполненные задания

### 1. Запуск системы Apache Airflow

**🔧 Bare Metal Implementation:** Система Apache Airflow развернута в Kubernetes кластере на виртуальных машинах Proxmox VE с использованием ArgoCD и Helm чарта (вместо Yandex Cloud Managed Service for Apache Airflow).

**Конфигурационные файлы:**
- [ArgoCD манифест для Airflow](./argocd/airflow/airflow-helm.yaml)
- [Dockerfile для кастомного образа Airflow](./argocd/airflow/Dockerfile)
- [Requirements.txt с зависимостями](./argocd/airflow/requirements.txt)

**Результат развертывания:**

![Airflow в ArgoCD](images/argocd-airflow.png)

**Web-интерфейс Airflow:**

![Airflow UI](images/airflow-ui.png)

### 2. Запуск системы MLflow

**🔧 Bare Metal Implementation:** Система MLflow развернута в Kubernetes кластере на виртуальных машинах Proxmox VE с использованием PostgreSQL в качестве базы данных метаданных и MinIO в качестве S3-совместимого хранилища артефактов (вместо отдельной ВМ в Yandex Cloud и Managed Service for PostgreSQL).

**Конфигурационные файлы:**
- [ArgoCD манифесты для MLflow](./argocd/mlflow/)

**Web-интерфейс MLflow:**

![MLflow UI](images/mkflow-ui.png)

### 3. Создание Python скрипта с PySpark

Разработан DAG для Apache Airflow, включающий обучение модели с использованием PySpark на данных о мошеннических транзакциях.

**Основные файлы:**
- [DAG файл simple_training_pipeline.py](./dags/simple_training_pipeline.py) - основной пайплайн обучения модели
- [Файл переменных Airflow](./dags/airflow_variables.json) - конфигурация подключений и параметров

**Ключевые особенности реализации:**
- Использование PySpark для обработки больших данных
- Обучение модели RandomForestClassifier для детекции мошенничества  
- Логирование метрик и параметров в MLflow
- Автоматическая регистрация лучших моделей

```python
# Фрагмент кода обучения модели
classifier = RandomForestClassifier(
    labelCol="tx_fraud",
    featuresCol="features", 
    numTrees=10,
    maxDepth=5
)

# Логирование в MLflow
mlflow.log_param("numTrees", 10)
mlflow.log_param("maxDepth", 5)
mlflow.log_metric("accuracy", accuracy)
mlflow.log_metric("auc", auc)
```

### 4. Сохранение метрик и артефактов в S3 хранилище

**🔧 Bare Metal Implementation:** Настроено сохранение всех артефактов MLflow в MinIO (S3-совместимое хранилище), развернутом в Kubernetes кластере на базе Proxmox VE (вместо Yandex Object Storage):

**Конфигурация подключения:**
```json
{
  "S3_ENDPOINT_URL": "http://192.168.31.201:9000",
  "S3_ACCESS_KEY": "admin", 
  "S3_SECRET_KEY": "password",
  "S3_BUCKET_NAME": "mlflow-artifacts"
}
```

**Результаты сохранения в MinIO:**

![MinIO Buckets](images/minio-buckets.png)

### 5. Периодическое выполнение DAG и тестирование

DAG настроен на периодическое выполнение каждый час и успешно протестирован:

```python
# Настройка расписания в DAG
dag = DAG(
    'simple_fraud_detection_training',
    default_args=default_args,
    description='Простой DAG для обучения модели мошенничества',
    schedule_interval=timedelta(hours=1),  # Каждый час
    start_date=datetime(2024, 7, 27),
    catchup=False,
)
```

**Результаты успешного выполнения:**

![MLflow Training Results](images/mlflow-training_20250727_1113.png)

Как видно на скриншоте, модель успешно обучена и зарегистрирована в MLflow с следующими метриками:
- **Accuracy**: 0.928 (92.8%)
- **AUC**: 0.847 (84.7%)
- **Precision**: 0.851 (85.1%)
- **Recall**: 0.847 (84.7%)

## Структура проекта

```
my-practice/
├── argocd/                           # ArgoCD манифесты
│   ├── airflow/                      # Конфигурация Airflow
│   │   ├── airflow-helm.yaml         # Helm чарт для Airflow
│   │   ├── Dockerfile                # Кастомный образ с зависимостями
│   │   ├── requirements.txt          # Python зависимости
│   │   └── README.md                 # Описание компонента
│   └── mlflow/                       # Конфигурация MLflow
│       └── applications-mlflow/      # ArgoCD приложения
├── dags/                             # Airflow DAG файлы
│   ├── simple_training_pipeline.py   # Основной пайплайн обучения
│   ├── airflow_variables.json        # Переменные конфигурации
│   └── README.md                     # Описание DAG
├── images/                           # Скриншоты результатов
│   ├── airflow-ui.png               # Web-интерфейс Airflow
│   ├── argocd-airflow.png           # Airflow в ArgoCD
│   ├── mkflow-ui.png                # Web-интерфейс MLflow
│   ├── minio-buckets.png            # Хранилище MinIO
│   └── mlflow-training_20250727_1113.png # Результаты обучения
└── README.md                         # Данный файл
```

## Использованные технологии

- **Apache Airflow** - оркестрация ML пайплайнов
- **MLflow** - отслеживание экспериментов и управление моделями
- **PySpark** - обработка данных и машинное обучение
- **MinIO** - S3-совместимое хранилище артефактов
- **Kubernetes** - контейнерная оркестрация
- **ArgoCD** - GitOps для развертывания приложений
- **PostgreSQL** - база данных метаданных MLflow

## 🏗️ Архитектурные особенности Bare Metal решения на Proxmox VE

**Отличия от требований задания (Yandex Cloud):**

| Компонент | Требование (Yandex Cloud) | Реализация (Bare Metal + Proxmox VE) |
|-----------|---------------------------|---------------------------------------|
| **Airflow** | Managed Service for Apache Airflow | Самостоятельное развертывание в K8s кластере |
| **MLflow** | Отдельная виртуальная машина | Pod в Kubernetes кластере |
| **База данных** | Managed Service for PostgreSQL/MySQL | PostgreSQL в контейнере |
| **Object Storage** | Yandex Object Storage | MinIO в K8s кластере |
| **Spark кластер** | Облачный Spark-кластер | PySpark в Airflow задачах |
| **Инфраструктура** | Облачные ресурсы | ВМ на Proxmox VE (физические серверы) |
| **Гипервизор** | Отсутствует (облачная абстракция) | **Proxmox VE** |

**Преимущества Bare Metal подхода:**
- ✅ Полный контроль над инфраструктурой
- ✅ Отсутствие зависимости от облачных провайдеров
- ✅ Возможность детальной настройки всех компонентов
- ✅ Экономия на облачных ресурсах
- ✅ Высокая производительность на выделенном железе

## Результаты работы

1. ✅ **Развернута система Apache Airflow** в Kubernetes кластере на ВМ Proxmox VE (вместо Yandex Managed Service)
2. ✅ **Развернута система MLflow** с PostgreSQL и MinIO storage на виртуальных машинах Proxmox VE
3. ✅ **Создан Python скрипт** с PySpark для обучения модели детекции мошенничества
4. ✅ **Настроено сохранение артефактов** в локальное S3-совместимое хранилище MinIO
5. ✅ **Настроено периодическое выполнение** DAG с успешными результатами обучения

**🎯 Все требования выполнены с использованием bare metal инфраструктуры на базе Proxmox VE вместо облачных сервисов Yandex Cloud**

**Достигнутые метрики модели:**
- Точность (Accuracy): **92.8%**
- Площадь под ROC-кривой (AUC): **84.7%**
- Precision: **85.1%**
- Recall: **84.7%**

Все компоненты системы интегрированы и работают в автоматическом режиме на собственной инфраструктуре на базе гипервизора Proxmox VE, обеспечивая регулярное переобучение модели на новых данных с отслеживанием качества через MLflow.

---

**💪 Заключение:** Домашнее задание выполнено с использованием bare metal решения на базе Proxmox VE вместо облачной инфраструктуры Yandex Cloud, что демонстрирует способность развертывать и управлять MLOps пайплайнами на собственной виртуализированной инфраструктуре. 