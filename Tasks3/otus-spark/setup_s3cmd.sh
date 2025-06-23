#!/bin/bash

# s3cmd --configure

# Установка переменных
MY_USER=ono
ACCESS_KEY=$(cat sa-key.json | jq -r .access_key.key_id | tr -d '"')
SECRET_KEY=$(cat sa-key.json | jq -r .secret | tr -d '"')

# Запись ключей в файл конфигурации s3cmd
sed -i "s/^access_key = .*/access_key = $ACCESS_KEY/" /home/$MY_USER/.s3cfg
sed -i "s/^secret_key = .*/secret_key = $SECRET_KEY/" /home/$MY_USER/.s3cfg
