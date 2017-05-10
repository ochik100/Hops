import json

import pyrebase


# with open('secret.json') as f:
#     data = json.load(f)


def connect_to_database():
    firebase = pyrebase.initialize_app(config)
    auth = firebase.auth()
    admin = auth.sign_in_with_email_and_password(email, password)
    token = admin['idToken']
    db = firebase.database()
    print "Connected to Hops database."
    return token, db
