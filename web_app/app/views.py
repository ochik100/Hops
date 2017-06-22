import os

import numpy as np
from flask import flash, redirect, render_template, request

from app import app
from firebase_connection import connect_to_database
from settings import APP_STATIC

token, db = connect_to_database()


@app.route('/results', methods=['GET', 'POST'])
def index():
    results = []
    beer_name = ""
    if request.method == "POST":
        beer_index = request.form.get('index')
        print "BEER INDEX - ", beer_index
        beer = db.child('beers').child(beer_index).get(token).val()
        beer_name = [beer['beer_name'], beer['brewery_name'], beer['top1'], beer['top2'], beer['top3'], beer['top4'], beer['top5'],
                     beer['top6'], beer['top7'], beer['beer_style']]
        indexes = np.array([beer['first'], beer['second'], beer[
                           'third'], beer['fourth'], beer['fifth']])
        for idx in indexes:
            details = db.child('beers').child(idx).get(token).val()
            output = unicode_helper((details['beer_name'],
                                     details['brewery_name'],
                                     details['beer_style'],
                                     details['top1'],
                                     details['top2'],
                                     details['top3'],
                                     details['top4'],
                                     details['top5'],
                                     details['top6'],
                                     details['top7']))
            results.append(output)
    return render_template("index.html", beer_name=beer_name, results=results)


def unicode_helper(output):
    return map(lambda x: x.encode('utf-8', 'ignore').decode('utf-8'), output)


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
