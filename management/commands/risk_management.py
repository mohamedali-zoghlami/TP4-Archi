from django.core.management.base import BaseCommand
import pika
import requests

class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=settings.RABBITMQ_HOST)
        )
        channel = connection.channel()
        channel.queue_declare(queue='Com-Risk')

        def callback(ch, method, properties, body):
            message_data = json.loads(body.decode())
            cin = message_data['cin']
            initial_score = message_data['initial_score']
            final_score = self.get_final_score(cin,initial_score)
            self.publish_final_score(cin,final_score)

        channel.basic_consume(queue='Com-Risk', on_message_callback=callback, auto_ack=True)
        print('Waiting for messages...')
        channel.start_consuming()

    def get_final_score(self, cin, initial_score):
        # response = requests.get('bank_central_api', params={'cin': cin})
        # client_data = Feature from response + the inital_score
        # final_score = ml_model.predict(client_data)
        final_score = 100
        return final_score

    def publish_final_score(self, cin, final_score):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=settings.RABBITMQ_HOST)
        )
        channel = connection.channel()
        channel.queue_declare(queue='Risk-Cred')
        message = {'cin': cin, 'final_score': final_score}
        channel.basic_publish(exchange='', routing_key='Risk-Cred', body=json.dumps(message))
        print('Final score published:', final_score)
        connection.close()