#!/bin/bash
source .bashrc

# Проверка наличия аргумента
vm_name=$1

# Установка переменных
YC_ZONE=ru-central1-a
YC_SUBNET_NAME=otus-dataproc-subnet
YC_SA_NAME=otus
YC_USER=osipov

# Создание виртуальной машины
log "Creating virtual machine..."
yc compute instance create \
    --preemptible \
    --name $vm_name \
    --hostname $vm_name \
    --zone ${YC_ZONE} \
    --memory=16 \
    --cores=2 \
    --create-boot-disk `
        `image-folder-id=standard-images,`
        `image-family=ubuntu-2004-lts,`
        `type=network-hdd,`
        `size=30 \
    --network-interface subnet-name=${YC_SUBNET_NAME},nat-ip-version=ipv4 \
    --service-account-name ${YC_SA_NAME} \
    --metadata serial-port-enable=1 \
    --metadata-from-file user-data=metadata.yaml

log "Virtual machine created successfully!"

# Ожидание завершения создания виртуальной машины

# Получение публичного IP-адреса виртуальной машины
log "Getting public IP address of the proxy VM..."
YC_PROXY_VM_PUBLIC_IP=$(
    yc compute instance get $vm_name \
        --format json | jq -r .network_interfaces[0].primary_v4_address.one_to_one_nat.address
)
log "Proxy VM public IP: $YC_PROXY_VM_PUBLIC_IP"

# Копирование SSH-ключа на виртуальную машину
scp ~/.ssh/yandex_cloud $YC_USER@$YC_PROXY_VM_PUBLIC_IP:~/.ssh/dataproc
log "INFO": "SSH private key copied to proxy VM successfully!"