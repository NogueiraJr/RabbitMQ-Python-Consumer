import pika
import requests

def callback(ch, method, properties, body):
  url = body.decode()
  response = requests.get(url)
  # Processar a resposta da API
  print(response.json())

credentials = pika.PlainCredentials('guest', 'guest')

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials)
)
channel = connection.channel()

channel.queue_declare(queue='user_data_queue')

channel.basic_consume(queue='user_data_queue', on_message_callback=callback, auto_ack=True)

print('Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
