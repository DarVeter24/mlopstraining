#!/bin/bash
file_path=$1

source .bashrc

# Установка переменных
YC_BUCKET=otus-bucket-b1gtvbvk47oh4lr4r4cb

# Загрузка данных в бакет
log "Uploading data to bucket..."
s3cmd put $file_path s3://$YC_BUCKET

# Проверка загрузки данных
log "Checking data in bucket..."
s3cmd ls s3://$YC_BUCKET
