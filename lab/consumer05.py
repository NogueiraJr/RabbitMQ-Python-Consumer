import pika
import json
import requests

# Conexão com o RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declarar as filas
channel.queue_declare(queue='user_queue')
channel.queue_declare(queue='response_queue')

# Mapeamento de métodos HTTP para funções requests
http_methods = {
    "POST": requests.post,
    "GET": requests.get,
    "DELETE": requests.delete,
    "PUT": requests.put,
    "PATCH": requests.patch
}

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
    
    # Obter a função de requests correspondente ao método HTTP
    request_function = http_methods.get(http_method)

    if request_function is None:
        print(f"Método HTTP {http_method} não suportado.")
        return
    
    # Realizar a chamada HTTP
    if http_method == "GET":
        response = request_function(endpoint, headers=headers, params=payload)
    else:
        response = request_function(endpoint, headers=headers, data=json.dumps(payload))
    
    # Preparar a resposta
    response_message = {
        "status_code": response.status_code,
        "text": response.text
    }

    # Publicar a resposta na fila de respostas com o mesmo correlation_id
    channel.basic_publish(exchange='',
                          routing_key=properties.reply_to,
                          properties=pika.BasicProperties(correlation_id=properties.correlation_id),
                          body=json.dumps(response_message))

    print(f"Resposta do servidor: {response.status_code} - {response.text}")

# Configurar o consumidor para ouvir a fila
channel.basic_consume(queue='user_queue', on_message_callback=callback, auto_ack=True)

print('Aguardando mensagens. Para sair, pressione CTRL+C')
channel.start_consuming()
