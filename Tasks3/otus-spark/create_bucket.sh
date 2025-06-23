#!/bin/bash
source .bashrc

# Установка переменных
YC_BUCKET=otus-dataproc-bucket-osipov

# Создание бакета
log "Creating bucket $YC_BUCKET..."
yc storage bucket create $YC_BUCKET --async
