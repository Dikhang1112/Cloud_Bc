from flask import Blueprint, render_template

auth = Blueprint('auth', __name__)

@auth.route('/API')
def newSource():
    return render_template("apiDb.html")

@auth.route('/s3')
def signUp():
    return render_template("awsS3.html")

@auth.route('/sche')
def scheduleTemplate():
    return  render_template("schedule.html")