# function log() {
#     sep="----------------------------------------------------------"
#     echo "[$(date +'%Y-%m-%d %H:%M:%S')] $sep " | tee -a $HOME/user_data_execution.log
#     echo "[$(date +'%Y-%m-%d %H:%M:%S')] [INFO] $1" | tee -a $HOME/user_data_execution.log
# }

# log "Starting user data script execution"

# s3cmd cp --config=/home/ubuntu/.s3cfg --acl-public s3://otus-mlops-source-data/2022-11-04.txt s3://otus-bucket-b1gtvbvk47oh4lr4r4cb/2022-11-04.txt

# файл для проверки