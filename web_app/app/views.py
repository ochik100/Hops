import os

import numpy as np
from flask import flash, redirect, render_template, request

from app import app
from settings import APP_STATIC


@app.route('/', methods=['GET', 'POST'])
@app.route('/index', methods=['GET', 'POST'])
def index():
    recommendations = np.load(file=os.path.join(APP_STATIC, 'data/five_most_similar_beers.npy'))
    # print recommendations
    results = None
    if request.method == "POST":
        beer_index = request.form.get('index')
        results = recommendations[beer_index]
        print results
    # results = {}
    # if request.method == "POST":
    #     try:
    #         beer_name = request.form['beer_name']
    return render_template("index.html", results=results)


@app.route('/about')
def about():
    return render_template("about.html")


@app.route('/beer_styles')
def beer_styles():
    return render_template("beer_styles.html")


@app.route('/test', methods=['GET'])
def test():
    return render_template('test.html')
