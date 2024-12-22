import pika
import json
import os
 
 
# Путь к файлу логирования
log_file = "./logs/metric_log.csv"

# Создаём файл логирования, если он не существует, и записываем заголовок
if not os.path.exists(log_file):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    with open(log_file, "w") as f:
        f.write("id,y_true,y_pred,absolute_error\n")


# Временное хранилище для данных
buffer = {}

# Функция для обновления логов
def update_log(data_type, data):
    global buffer, log_df

    # Получаем id и значение
    msg_id = data["id"]
    value = data["body"]

    if msg_id not in buffer:
        buffer[msg_id] = {"y_true": None, "y_pred": None}

    # Обновляем буфер
    buffer[msg_id][data_type] = value

    # Если в буфере есть обе метки, вычисляем абсолютную ошибку
    if buffer[msg_id]["y_true"] is not None and buffer[msg_id]["y_pred"] is not None:
        y_true = buffer[msg_id]["y_true"]
        y_pred = buffer[msg_id]["y_pred"]
        absolute_error = abs(y_true - y_pred)

        # Записываем в лог
        with open(log_file, "a") as f:
            f.write(f"{msg_id},{y_true},{y_pred},{absolute_error}\n")

        # Удаляем данные из буфера
        del buffer[msg_id]
        
        
        
    # Создаём функцию callback для обработки данных из очереди
def callback(ch, method, properties, body):
    try:
        print(f'Из очереди {method.routing_key} получено значение {json.loads(body)}')        
        data = json.loads(body)
        if method.routing_key == "y_true":
            update_log("y_true", data)
        elif method.routing_key == "y_pred":
            update_log("y_pred", data)
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
