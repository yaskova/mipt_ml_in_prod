import pika
import json
import os

# Путь к файлу логирования
log_file = "./logs/metric_log.csv"

# Проверяем, существует ли файл логирования. Если нет, создаем и инициализируем его заголовком
if not os.path.exists(log_file):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    with open(log_file, "w") as f:
        f.write("id,y_true,y_pred,absolute_error\n")

# Создаем временное хранилище для данных. Сюда они будут попадать из очереди и храниться здесь до расчета абсолютной ошибки
temp = {}

# Функция для обновления логов
def update_log(data_type, data):
    global temp

    # Разбираем сообщение на идентификатор и тело сообщения (значение)
    message_id = data["id"]
    message_body = data["body"]

    # Проверяем, не было ли еще во временном хранилище сообщения с таким идентификатором, чтобы избежать дубликатов
    # Если нет, создаем заготовку для записи данных по данному идентификатору
    if message_id not in temp:
        temp[message_id] = {"y_true": None, "y_pred": None}

    # Добавляем данные во временное хранилище
    temp[message_id][data_type] = message_body

    # Если во временном есть y_true и y_pred, то вычисляем абсолютную ошибку
    if temp[message_id]["y_true"] is not None and temp[message_id]["y_pred"] is not None:
        y_true = temp[message_id]["y_true"]
        y_pred = temp[message_id]["y_pred"]
        absolute_error = abs(y_true - y_pred)

        # Записываем в лог данные вместе с рассчитанной ошибкой
        with open(log_file, "a") as f:
            f.write(f"{message_id},{y_true},{y_pred},{absolute_error}\n")

        # Удаляем из временного хранилища данные, с которыми закончили работу
        del temp[message_id]

# # Создаём функцию callback для обработки данных из очереди
def callback(ch, method, properties, body):
    try:
        print(f'Из очереди {method.routing_key} получено значение {json.loads(body)}')         
        data = json.loads(body)
        if method.routing_key == "y_true":
            update_log("y_true", data)
        elif method.routing_key == "y_pred":
            update_log("y_pred", data)
        print(f'Данные добавлены в лог')
    except Exception as e:
        print(f"Ошибка обработки сообщения: {e}")
 
try:
    # Создаём подключение к серверу на локальном хосте
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
   
    # Объявляем очередь y_true
    channel.queue_declare(queue='y_true')
    # Объявляем очередь y_pred
    channel.queue_declare(queue='y_pred')
 
    # # Создаём функцию callback для обработки данных из очереди
    # def callback(ch, method, properties, body):
    #     print(f'Из очереди {method.routing_key} получено значение {json.loads(body)}')
 
    # Извлекаем сообщение из очереди y_true
    channel.basic_consume(
        queue='y_true',
        on_message_callback=callback,
        auto_ack=True
    )
    # Извлекаем сообщение из очереди y_pred
    channel.basic_consume(
        queue='y_pred',
        on_message_callback=callback,
        auto_ack=True
    )
 
    # Запускаем режим ожидания прихода сообщений
    print('...Ожидание сообщений, для выхода нажмите CTRL+C')
    channel.start_consuming()
except:
    print('Не удалось подключиться к очереди')
