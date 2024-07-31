import pika
import json
import requests

# Conexão com o RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declarar a fila
channel.queue_declare(queue='hello')

# Função callback para processar as mensagens recebidas
def callback(ch, method, properties, body):
    print("Mensagem recebida: %r" % body)
    
    # Parse da mensagem
    message = json.loads(body)
    
    # Extrair o endpoint, os cabeçalhos e o payload
    endpoint = message.get("endpoint")
    headers = message.get("headers")
    payload = message.get("payload")
    
    # Realizar a chamada HTTP
    response = requests.post(
        endpoint,
        headers=headers,
        data=json.dumps(payload)
    )
    
    print(f"Resposta do servidor: {response.status_code} - {response.text}")

# Configurar o consumidor para ouvir a fila
channel.basic_consume(queue='hello', on_message_callback=callback, auto_ack=True)

print('Aguardando mensagens. Para sair, pressione CTRL+C')
channel.start_consuming()
