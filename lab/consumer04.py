import pika
import json
import requests

# Conexão com o RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declarar a fila
channel.queue_declare(queue='user_queue')

# Função callback para processar as mensagens recebidas
def callback(ch, method, properties, body):
    print("Mensagem recebida: %r" % body)
    
    # Parse da mensagem
    message = json.loads(body)
    
    # Extrair o método HTTP, o endpoint, os cabeçalhos e o payload
    http_method = message.get("method")
    endpoint = message.get("endpoint")
    headers = message.get("headers")
    payload = message.get("payload")
    
    # Realizar a chamada HTTP com base no método especificado
    if http_method == "POST":
        response = requests.post(endpoint, headers=headers, data=json.dumps(payload))
    elif http_method == "GET":
        response = requests.get(endpoint, headers=headers, params=payload)
    elif http_method == "DELETE":
        response = requests.delete(endpoint, headers=headers, data=json.dumps(payload))
    # Adicionar outros métodos HTTP conforme necessário
    else:
        print(f"Método HTTP {http_method} não suportado.")
        return
    
    print(f"Resposta do servidor: {response.status_code} - {response.text}")

# Configurar o consumidor para ouvir a fila
channel.basic_consume(queue='user_queue', on_message_callback=callback, auto_ack=True)

print('Aguardando mensagens. Para sair, pressione CTRL+C')
channel.start_consuming()
