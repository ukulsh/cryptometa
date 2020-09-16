
from project.api.models import User, Client


def based_user_register(data):
    username = data.get('username')
    first_name = data.get('first_name')
    last_name = data.get('last_name')
    password = data.get('password')
    email = data.get('primary_email')
    tabs = data.get('tabs')
    phone_number = data.get('phone_no')
    client_prefix = data.get('client_prefix')
    calling_active = True if data.get('calling_active') else False
    client = Client.query.filter_by(client_prefix=client_prefix).first()
    user = User(username=username, email=email, password=password, first_name=first_name, last_name=last_name, tabs=tabs,
                calling_active=calling_active, client_id=client.id, group_id=2, phone_number=phone_number)
    return user


def user_register(data, user_id):
    username = data.get('username')
    first_name = data.get('first_name')
    last_name = data.get('last_name')
    password = data.get('password')
    email = data.get('email')
    tabs = data.get('tabs')
    phone_number = data.get('phone_no')
    calling_active = data.get('calling_active')
    source_user = User.query.filter_by(id=user_id).first()
    user = User(username=username, email=email, password=password, first_name=first_name, last_name=last_name, tabs=tabs,
                calling_active=calling_active, client_id=source_user.client_id, group_id=2, phone_number=phone_number)
    return user