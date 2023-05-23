import pika
from mongoengine import connect
from models import Client
from faker import Faker

connect(host="mongodb+srv://hw8:567432@cluster0.g08jtiw.mongodb.net/hw8?retryWrites=true&w=majority")
fake = Faker("uk_UA")


def seed_client(number_contacts):
    for _ in range(number_contacts):
        fullname = fake.name()
        email = fake.email()
        phone_number = fake.phone_number()
        address = fake.address()
        preferred_method = fake.random_element(elements=("Email", "SMS"))
        client = Client(fullname=fullname,
                        email=email,
                        phone_number=phone_number,
                        address=address,
                        preferred_method=preferred_method)
        client.save()
        print(f"Contact {client.fullname} successfully added")


def send_message():
    credentials = pika.PlainCredentials("guest", "guest")
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost", port=5672, credentials=credentials))

    channel = connection.channel()
    channel.queue_declare(queue="email_queue")
    channel.queue_declare(queue="sms_queue")

    clients = Client.objects()
    for client in clients:
        message = str(clients.id)
        if client.preferred_method == "Email":
            channel.basic_publish(exchange="", routing_key="email_queue", body=message)
            print(f"[v]Message for {client.fullname} has been sent")
        else:
            channel.basic_publish(exchange="", routing_key="sms_queue", body=message)
            print(f"[v]Message for {client.fullname} has been sent")
    connection.close()


if __name__ == '__main__':
    seed_client(20)
    send_message()
