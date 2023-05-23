from mongoengine import connect, Document, StringField, BooleanField

connect(host="mongodb+srv://hw8:567432@cluster0.g08jtiw.mongodb.net/hw8?retryWrites=true&w=majority")


class Client(Document):
    fullname = StringField(max_length=200, required=True)
    email = StringField(max_length=100, required=True)
    phone_number = StringField(max_length=20, required=True)
    address = StringField(max_length=100, required=True)
    sent_message = BooleanField(default=False)
    preferred_method = StringField(required=True)
