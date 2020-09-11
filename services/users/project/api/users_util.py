
from project.api.models import User, Client


def register_user(data):
    username = data.get('username')
    first_name = data.get('first_name')
    last_name = data.get('last_name')
    password = data.get('password')
    email = data.get('primary_email')
    tabs = data.get('tabs')
    phone_number = data.get('phone_number')
    client_prefix = data.get('client_prefix')
    calling_active = True if data.get('calling_active') else False
    client = Client.query.filter_by(client_prefix=client_prefix).first()
    user = User(username=username, email=email, password=password, first_name=first_name, last_name=last_name, tabs=tabs,
                calling_active=calling_active, client_id=client.id, group_id=2, phone_number=phone_number)
    return user