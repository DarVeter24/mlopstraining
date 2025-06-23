tf_init:
	cd infrastructure && terraform init -upgrade

tf_plan:
	cd infrastructure && terraform plan

tf_apply:
	cd infrastructure && terraform apply -auto-approve

tf_destroy:
	cd infrastructure && terraform destroy -auto-approve

yc_list_images:
	yc compute image list --folder-id standard-images > images.txt

sync-repo:
	rsync -avz \
		--exclude=.venv \
		--exclude=infrastructure/.terraform \
		--exclude=.terraform \
		--exclude=.terraform-version \
		--exclude=.terraformrc \
		--exclude=*.info \
		--exclude=*.lock.hcl \
		--exclude=*.tfstate \
		--exclude=*.backup \
		--exclude=*.json . yc-proxy:/home/ubuntu/otus/otus-practice-cloud-infra
