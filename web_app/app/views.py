from flask import flash, redirect, render_template

from app import app


@app.route('/')
@app.route('/index')
def index():
    return render_template("index.html")


@app.route('/about')
def about():
    return render_template("about.html")


@app.route('/beer_styles')
def beer_styles():
    return render_template("beer_styles.html")


@app.route('/test', methods=['GET'])
def test():
    return render_template('test.html')
