

Для выполнения задач использовал исходники с https://github.com/NickOsipov/otus-practice-cloud-infra/tree/main/infra


## Task 2

1. Создать новый backet в Yandex Cloud Object Storage с использованием terraform скрипта. Выложить созданный вами скрипт в github репозиторий c заданием.

Бакет создается через https://github.com/DarVeter24/mlopstraining/blob/main/Tasks2/otus-practice-cloud-infra/infra/main.tf


2. Скопировать в него содержимое предоставленного Вам хранилища с
использованием инструмента s3cmd.

Копирование также осуществлялось за счет скрипта user_data.sh, в котором сделано небольшое изменение, поменяли копирование одного файла на все.

https://github.com/DarVeter24/mlopstraining/blob/main/Tasks2/otus-practice-cloud-infra/infra/scripts/user_data.sh


Посмотреть можно

`s3cmd ls s3://otus-bucket-b1g51smi0tng33t5hkg9`

`https://storage.yandexcloud.net/otus-bucket-b1g51smi0tng33t5hkg9`


3. Создать Spark-кластер в Data Proc с двумя подкластерами со следующими
характеристиками:
а) Мастер-подкластер: класс хоста s3-c2-m8, размер хранилища 40 ГБ.
б) Data-подкластер: класс хоста s3-c4-m16, 3 хоста, размер хранилища128 ГБ.
Для экономии ресурсов необходимо использовать terraform скрипт для создания и
удаления кластера.


![](Tasks2/images/bucket.png)






