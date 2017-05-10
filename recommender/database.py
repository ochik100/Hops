import json

import pyrebase

with open('secret.json') as f:
    data = json.load(f)

config = {"apiKey": data['apiKey'],
          "authDomain": data['authDomain'],
          "databaseURL": data['databaseURL'],
          "storageBucket": data['storageBucket']}

email = data['email']
password = data['password']


def connect_to_database():
    firebase = pyrebase.initialize_app(config)
    auth = firebase.auth()
    admin = auth.sign_in_with_email_and_password(email, password)
    token = admin['idToken']
    db = firebase.database()
    return token, db
