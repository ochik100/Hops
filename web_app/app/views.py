import os

import numpy as np
from flask import flash, redirect, render_template, request

from app import app
from firebase_connection import connect_to_database
from settings import APP_STATIC

token, db = connect_to_database()


@app.route('/results', methods=['GET', 'POST'])
def index():

    # recommendations = np.load(file=os.path.join(APP_STATIC, 'data/five_most_similar_beers.npy'))
    # print recommendations
    results = []
    beer_name = ""
    if request.method == "POST":
        beer_index = request.form.get('index')
        beer = db.child('beers').child(beer_index).get(token).val()
        beer_name = [beer['beer_name'], beer['brewery_name']]
        indexes = np.array([beer['first'], beer['second'], beer[
                           'third'], beer['fourth'], beer['fifth']])
        for idx in indexes:
            details = db.child('beer_names').child(idx).get(token).val()
            output = (details['beer_name'].encode('utf-8'),
                      details['brewery_name'].encode('utf-8'))
            results.append(output)
    return render_template("index.html", beer_name=beer_name, results=results)


@app.route('/', methods=['GET', 'POST'])
@app.route('/search', methods=['GET', 'POST'])
def search():
    return render_template("search.html")


@app.route('/about')
def about():
    return render_template("about.html")


@app.route('/beer_styles')
def beer_styles():
    return render_template("beer_styles.html")


@app.route('/test', methods=['GET'])
def test():
    return render_template('test.html')
