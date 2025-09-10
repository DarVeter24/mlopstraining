#!/bin/bash

echo "🔍 Tasks10 ML Service - Базовая диагностика"
echo "=============================================="

echo ""
echo "1️⃣ Проверка ArgoCD развертывания:"
echo "-----------------------------------"

echo "📋 Проверка ArgoCD Project:"
kubectl get appproject tasks10-ml-service -n argocd 2>/dev/null && echo "✅ ArgoCD Project существует" || echo "❌ ArgoCD Project не найден"

echo ""
echo "📋 Проверка ArgoCD Application:"
kubectl get application tasks10-ml-service -n argocd 2>/dev/null && echo "✅ ArgoCD Application существует" || echo "❌ ArgoCD Application не найден"

echo ""
echo "📋 Статус ArgoCD Application:"
kubectl get application tasks10-ml-service -n argocd -o jsonpath='{.status.sync.status}' 2>/dev/null && echo " - Sync Status" || echo "❌ Не удается получить статус синхронизации"

echo ""
echo "2️⃣ Проверка ресурсов Kubernetes:"
echo "--------------------------------"

echo "📦 Проверка namespace:"
kubectl get namespace mlops-tasks10 2>/dev/null && echo "✅ Namespace mlops-tasks10 существует" || echo "❌ Namespace mlops-tasks10 не найден"

echo ""
echo "🚀 Проверка подов:"
kubectl get pods -n mlops-tasks10 2>/dev/null || echo "❌ Не удается получить список подов"

echo ""
echo "🔗 Проверка сервисов:"
kubectl get svc -n mlops-tasks10 2>/dev/null || echo "❌ Не удается получить список сервисов"

echo ""
echo "📊 Проверка deployment:"
kubectl get deployment -n mlops-tasks10 2>/dev/null || echo "❌ Не удается получить список deployments"

echo ""
echo "3️⃣ Проверка HPA:"
echo "----------------"
kubectl get hpa -n mlops-tasks10 2>/dev/null || echo "❌ HPA не найден"

echo ""
echo "4️⃣ Проверка доступности API:"
echo "----------------------------"

echo "🔍 Проверка сервиса через port-forward (запустите в отдельном терминале):"
echo "kubectl port-forward svc/tasks10-ml-service-service 8000:80 -n mlops-tasks10"

echo ""
echo "🌐 Проверка внешнего доступа:"
echo "curl -I http://tasks10-ml-api.darveter.com/health"

echo ""
echo "📊 Проверка метрик:"
echo "curl -I http://tasks10-ml-api.darveter.com/metrics"

echo ""
echo "5️⃣ Проверка логов подов:"
echo "------------------------"
echo "kubectl logs -n mlops-tasks10 -l app=tasks10-ml-api --tail=10"

echo ""
echo "=============================================="
echo "✅ Скрипт диагностики завершен"
echo "Запустите команды выше для детальной проверки"
