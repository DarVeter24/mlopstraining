"""
Примеры использования скриптов для MLOps pipeline
"""

from train import train_base_model
from ab_test import run_ab_test
from common import setup_mlflow, load_model_from_mlflow


def example_full_pipeline():
    """
    Пример полного MLOps pipeline
    
    Выполняет полный цикл:
    1. Обучение базовой модели
    2. A/B тестирование с оптимизацией
    3. Автоматическое развертывание
    
    Returns
    -------
    ab_results : dict
        Результаты A/B тестирования
    """
    print("ЗАПУСК ПОЛНОГО MLOPS PIPELINE")
    print("="*50)
    
    # Настройка MLflow
    setup_mlflow()
    
    # 1. Обучение базовой модели
    print("\nШАГ 1: Обучение базовой модели")
    model, metrics = train_base_model(
        model_name="url_classifier_demo",
        register_as_prod=True
    )
    
    # 2. A/B тестирование
    print("\nШАГ 2: A/B тестирование")
    ab_results = run_ab_test(
        model_name="url_classifier_demo",
        n_iter=30,  # Меньше итераций для демо
        auto_deploy=True  # Автоматическое развертывание
    )
    
    # 3. Проверка результата
    print("\nШАГ 3: Проверка финальной модели")
    final_model = load_model_from_mlflow("url_classifier_demo", alias="champion")
    
    if final_model:
        print("Pipeline выполнен успешно!")
        print(f"Финальная модель в Production готова к использованию")
    else:
        print("Ошибка в pipeline")
    
    return ab_results


def example_manual_workflow():
    """
    Пример ручного workflow для контролируемого развертывания
    
    Выполняет обучение и тестирование без автоматического развертывания,
    оставляя решение о развертывании на усмотрение пользователя.
    
    Returns
    -------
    ab_results : dict
        Результаты A/B тестирования
    """
    print("РУЧНОЙ WORKFLOW")
    print("="*30)
    
    # 1. Обучение без автоматической регистрации в Production
    print("Обучение модели без автоматической регистрации...")
    train_base_model(
        model_name="url_classifier_manual",
        register_as_prod=False  # Не регистрируем сразу в Production
    )
    
    # 2. A/B тест без автоматического развертывания
    print("A/B тест без автоматического развертывания...")
    ab_results = run_ab_test(
        model_name="url_classifier_manual",
        auto_deploy=False  # Ручное принятие решения
    )
    
    # 3. Ручное принятие решения
    if ab_results and ab_results['should_deploy']:
        print("Рекомендуется развернуть новую модель")
        print("   Выполните ручное развертывание при необходимости")
    else:
        print("Рекомендуется оставить текущую модель")
    
    return ab_results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Примеры использования MLOps pipeline")
    parser.add_argument("--workflow", choices=["full", "manual"], default="full", help="Тип workflow для демонстрации")
    
    args = parser.parse_args()
    
    if args.workflow == "full":
        example_full_pipeline()
    else:
        example_manual_workflow()
