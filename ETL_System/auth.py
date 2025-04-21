from flask import Blueprint, render_template

auth = Blueprint('auth', __name__)

@auth.route('/login')
def login():
    return render_template("login.html")


@auth.route('/API')
def newSource():
    return render_template("apiDb.html")

@auth.route('/s3')
def signUp():
    return render_template("awsS3.html")