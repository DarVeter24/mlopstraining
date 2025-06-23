SHELL := /bin/bash

include .env

.EXPORT_ALL_VARIABLES:

.PHONY: setup-airflow-variables
setup-airflow-variables:
	@echo "Running setup_airflow_variables.sh on $(AIRFLOW_HOST)..."
	ssh -i $(PRIVATE_KEY_PATH) \
		-o StrictHostKeyChecking=no \
		-o UserKnownHostsFile=/dev/null \
		$(AIRFLOW_VM_USER)@$(AIRFLOW_HOST) \
		'bash /home/ubuntu/setup_airflow_variables.sh'
	@echo "Script execution completed"

.PHONY: upload-dags-to-airflow
upload-dags-to-airflow:
	@echo "Uploading dags to $(AIRFLOW_HOST)..."
	scp -i $(PRIVATE_KEY_PATH) \
		-o StrictHostKeyChecking=no \
		-o UserKnownHostsFile=/dev/null \
		-r dags/*.py $(AIRFLOW_VM_USER)@$(AIRFLOW_HOST):/home/airflow/dags/
	@echo "Dags uploaded successfully"

.PHONY: upload-dags-to-bucket
upload-dags-to-bucket:
	@echo "Uploading dags to $(S3_BUCKET_NAME)..."
	s3cmd put --recursive dags/ s3://$(S3_BUCKET_NAME)/dags/
	@echo "DAGs uploaded successfully"

.PHONY: upload-src-to-bucket
upload-src-to-bucket:
	@echo "Uploading src to $(S3_BUCKET_NAME)..."
	s3cmd put --recursive src/ s3://$(S3_BUCKET_NAME)/src/
	@echo "Src uploaded successfully"

.PHONY: upload-data-to-bucket
upload-data-to-bucket:
	@echo "Uploading data to $(S3_BUCKET_NAME)..."
	s3cmd put --recursive data/input_data/*.csv s3://$(S3_BUCKET_NAME)/input_data/
	@echo "Data uploaded successfully"

upload-all: upload-data-to-bucket upload-src-to-bucket upload-dags-to-bucket

.PHONY: download-output-data-from-bucket
download-output-data-from-bucket:
	@echo "Downloading output data from $(S3_BUCKET_NAME)..."
	s3cmd get --recursive s3://$(S3_BUCKET_NAME)/output_data/ data/output_data/
	@echo "Output data downloaded successfully"

.PHONY: instance-list
instance-list:
	@echo "Listing instances..."
	yc compute instance list

.PHONY: git-push-secrets
git-push-secrets:
	@echo "Pushing secrets to github..."
	python3 utils/push_secrets_to_github_repo.py

sync-repo:
	rsync -avz \
		--exclude=.venv \
		--exclude=infra/.terraform \
		--exclude=*.tfstate \
		--exclude=*.backup \
		--exclude=*.json . yc-proxy:/home/ubuntu/otus/otus-practice-data-pipeline

sync-env:
	rsync -avz yc-proxy:/home/ubuntu/otus/otus-practice-data-pipeline/.env .env

airflow-cluster-mon:
	yc logging read --group-name=default --follow