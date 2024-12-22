import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import time

# Путь к директориям логов
log_file = "./logs/metric_log.csv"
output_image = "./logs/error_distribution.png"

# Убедимся, что папка для логов существует
os.makedirs("./logs", exist_ok=True)

def plot_error_distribution():
    while True:
        try:
            # Проверяем, существует ли log-файл
            if os.path.exists(log_file):
                # Читаем данные из metric_log.csv
                df = pd.read_csv(log_file)

                # Проверяем, есть ли данные для построения
                if 'absolute_error' in df.columns and not df.empty:
                    # Построение гистограммы абсолютных ошибок
                    plt.figure(figsize=(8, 6))
                    sns.histplot(
                        df["absolute_error"], 
                        bins=20, 
                        kde=True, 
                        label="Гистограмма"
                    )


                    plt.title("Распределение абсолютных ошибок", fontsize=16)
                    plt.xlabel("Абсолютная ошибка", fontsize=12)
                    plt.ylabel("Частота", fontsize=12)
                    plt.grid(True)

                    # Сохранение графика в файл
                    plt.savefig(output_image)
                    plt.close()
                    print(f"Гистограмма обновлена: {output_image}")

            else:
                print(f"Файл {log_file} не найден. Ожидание...")

        except Exception as e:
            print(f"Ошибка при обновлении графика: {e}")

        # Интервал обновления в секундах
        time.sleep(10)


plot_error_distribution()
