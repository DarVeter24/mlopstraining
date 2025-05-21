# OTUS. Настройка облачной инфраструктуры

## План занятия

1. Создадим инфраструктуру в Yandex.Cloud с помощью Terraform 
   1. Сервисный аккаунт
   2. Сеть
   3. Объектное хранилище S3
   4. Yandex Data Processing кластер
   5. ВМ для доступа к кластеру
 
2. Посмотрим на автоматическую загрузку данных в S3
3. Зайдем на мастерноду Yandex Data Processing и загрузим данные в HDFS

## Команды

Команды для работы с Terraform упакованы в Makefile:

```bash
make tf_init
make tf_apply
make tf_destroy
```

## Структура проекта

```
.
├── README.md                       # Документация проекта
├── Makefile                        # Команды для управления инфраструктурой
├── check.sh                        # Скрипт для проверки
└── infrastructure/                 # Директория с Terraform конфигурацией
    ├── main.tf                     # Основной файл с описанием ресурсов
    ├── variables.tf                # Определения переменных
    ├── outputs.tf                  # Выходные значения
    ├── provider.tf                 # Настройки провайдера
    ├── terraform.tfvars.example    # Пример файла с переменными
    └── scripts/                    # Скрипты для настройки инфраструктуры
        ├── user_data.sh            # Скрипт инициализации ВМ
        └── upload_data_to_hdfs.sh  # Скрипт загрузки данных в HDFS
```

## Предварительные требования

1. Установленный [Terraform](https://developer.hashicorp.com/terraform/downloads)
2. Аккаунт в [Yandex Cloud](https://cloud.yandex.ru/)
3. Настроенный [YC CLI](https://cloud.yandex.ru/docs/cli/quickstart)
4. SSH ключи для доступа к создаваемым ВМ

## Настройка

1. Скопируйте файл с примером переменных:
```bash
cp infrastructure/terraform.tfvars.example infrastructure/terraform.tfvars
```

2. Заполните переменные в файле `terraform.tfvars`:
- `yc_token` - OAuth токен
- `yc_cloud_id` - ID облака
- `yc_folder_id` - ID каталога
- `yc_image_id` - ID образа Ubuntu 20.04
- `public_key_path` - путь к публичному SSH ключу
- `private_key_path` - путь к приватному SSH ключу

## Развертывание инфраструктуры

1. Инициализация Terraform:
```bash
make tf_init
```

2. Применение конфигурации:
```bash
make tf_apply
```

После успешного применения конфигурации, в выводе будут показаны:
- IP адрес прокси-ВМ
- Имя созданного S3 бакета

## Подключение к кластеру

1. Подключитесь к прокси-ВМ:
```bash
ssh ubuntu@<proxy_public_ip>
```

2. С прокси-ВМ можно подключиться к мастер-ноде Dataproc:
```bash
ssh dataproc-master
```

## Удаление инфраструктуры

Для удаления всех созданных ресурсов выполните:
```bash
make tf_destroy
```

## Дополнительная информация

- [Документация Yandex Cloud](https://cloud.yandex.ru/docs)
- [Документация Terraform](https://developer.hashicorp.com/terraform/docs)
- [Документация Dataproc](https://cloud.yandex.ru/docs/data-proc)

## Лицензия

MIT

## Автор

- [Nick Osipov](https://t.me/NickOsipov)