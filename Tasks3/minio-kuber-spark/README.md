
http://jupyter.darveter.com/user/admin/

sudo pacman -S jdk17-openjdk
sudo archlinux-java set java-17-openjdk
http://jupyter.darveter.com/user/admin/


cat <<EOF > ~/.s3cfg.yandex
[default]
access_key = ${access_key}
secret_key = ${secret_key}
host_base = storage.yandexcloud.net
host_bucket = %(bucket)s.storage.yandexcloud.net
use_https = True
EOF


cat <<EOF > ~/.s3cfg.minio
[default]
access_key = admin
secret_key = password
host_base = 192.168.31.201:9000
host_bucket = 192.168.31.201:9000/%(bucket)
use_https = False
signature_v2 = False
EOF

s3cmd ls s3://otus --config ~/.s3cfg.minio


s3cmd sync --config ~/.s3cfg.minio s3://otus/clean ./clean

s3cmd ls s3://otus-bucket-b1g51smi0tng33t5hkg9/

s3cmd sync --config ~/.s3cfg ./clean s3://otus-bucket-b1g51smi0tng33t5hkg9







