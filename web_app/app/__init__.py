from flask import Flask
from flask_bootstrap import Bootstrap


app = Flask(__name__, static_url_path='/static')
Bootstrap(app)

from app import views
