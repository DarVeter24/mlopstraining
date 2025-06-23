# Объявление провайдера
terraform {
  required_providers {
    yandex = {
      source = "yandex-cloud/yandex"
    }
  }
  required_version = ">= 1.00"
}

provider "yandex" {
  zone      = var.yc_config.zone
  folder_id = var.yc_config.folder_id
  token     = var.yc_config.token
  cloud_id  = var.yc_config.cloud_id
}
