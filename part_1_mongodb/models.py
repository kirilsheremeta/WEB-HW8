from mongoengine import connect, Document, ListField, ReferenceField, StringField

connect(host="mongodb+srv://hw8:567432@cluster0.g08jtiw.mongodb.net/hw8?retryWrites=true&w=majority")


class Author(Document):
    fullname = StringField(required=True)
    date_of_birth = StringField()
    place_of_birth = StringField()
    description = StringField()


class Quotes(Document):
    author = ReferenceField(Author)
    quoter = StringField()
    tags = ListField(StringField())
