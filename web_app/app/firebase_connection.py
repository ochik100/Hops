import json

import pyrebase

# with open('secret.json') as f:
#     data = json.load(f)

config = {"apiKey": "AIzaSyCw6X5uPTCS51Lh4z1-NS4BBCJAJYa18pQ",
          "authDomain": "hops-66a78.firebaseapp.com",
          "databaseURL": "https://hops-66a78.firebaseio.com",
          "storageBucket": "hops-66a78.appspot.com"}

email = "kelly.ochikubo@gmail.com"
password = "hopsrecommender"


def connect_to_database():
    firebase = pyrebase.initialize_app(config)
    auth = firebase.auth()
    admin = auth.sign_in_with_email_and_password(email, password)
    token = admin['idToken']
    db = firebase.database()
    print "Connected to Hops database."
    return token, db
