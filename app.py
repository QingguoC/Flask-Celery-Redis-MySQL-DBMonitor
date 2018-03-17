#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar  2 19:22:47 2018

@author: qingguo
"""
from flask import Flask, render_template, redirect ,request, url_for
from flask_sqlalchemy import SQLAlchemy 
from celery import Celery
import datetime

from os.path import join, dirname
import os
from dotenv import load_dotenv

from celery.schedules import crontab
from datetime import datetime


dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)
SQLALCHEMY_DATABASE_URI = os.getenv('SQLALCHEMY_DATABASE_URI')
SECRET_KEY = os.getenv('SECRET_KEY')
CELERY_BROKER_URL = os.getenv('CELERY_BROKER_URL')
CELERY_BACKEND = os.getenv('CELERY_BACKEND')


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = SQLALCHEMY_DATABASE_URI
app.config['SECRET_KEY'] = SECRET_KEY
app.config['CELERY_BROKER_URL'] = CELERY_BROKER_URL
app.config['CELERY_BACKEND'] = CELERY_BACKEND

db = SQLAlchemy(app)
eng = db.engine
def make_celery(app):
    celery = Celery(app.import_name, backend=app.config['CELERY_BACKEND'],
                    broker=app.config['CELERY_BROKER_URL'])
    celery.conf.update(app.config)
    celery.conf.beat_schedule = {
    'every-minute': {
        'task': 'app.update',
        'schedule': crontab()
        },
    }
    TaskBase = celery.Task
    class ContextTask(TaskBase):
        abstract = True
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery.Task = ContextTask
    return celery
    
celery = make_celery(app)


@app.before_first_request
def setupDatabase():
    eng.execute("DROP TABLE IF EXISTS Chen.output_table")

    eng.execute("CREATE TABLE IF NOT EXISTS Chen.output_table  (`Column Name` varchar(20), Average double,`Standard Deviation` double, Median double ,Count int, timestamp varchar(20) )")


@app.route("/")
def index():
    eng.execute('use ApplicationData')
    q = eng.execute("select count(*) as ct from raw_data")
    return "ApplicationData.raw_data has " + str(q.fetchall()[0][0]) + " rows"


@app.route('/updateStats')
def updateStats():
    update.delay()
    return "sent request"
    
#==============================================================================
#  “lot_size_sqft”, “total_building_sqft”, “yr_built”, “bedrooms”, “total_rooms”, “bath_total”, “final_value”   
#==============================================================================

def update_statement(heading, count, timestamp):
    average = eng.execute("select avg({}) from ApplicationData.raw_data".format(heading)).fetchall()[0][0]
    std = eng.execute("select STDDEV({}) from ApplicationData.raw_data".format(heading)).fetchall()[0][0]
#==============================================================================
#     median = average
#==============================================================================
    median = eng.execute("SELECT AVG(middle_values) AS 'median' FROM (SELECT t1.{} AS 'middle_values' FROM(SELECT @row:=@row+1 as `row`, x.{} FROM ApplicationData.raw_data AS x, (SELECT @row:=0) AS r ORDER BY x.{} ) AS t1, ( SELECT COUNT(*) as 'count' FROM ApplicationData.raw_data x) AS t2 WHERE t1.row >= t2.count/2 and t1.row <= ((t2.count/2) +1)) AS t3".format(heading,heading,heading)).fetchall()[0][0]
    return "INSERT INTO Chen.output_table (`Column Name`, Average, `Standard Deviation`, Median, Count,timestamp) values ('"+heading+"',"+str(average)+","+str(std)+","+str(median)+","+str(count)+",'"+timestamp+"')"

@celery.task(name='app.update')
def update():
    
    maxCount = eng.execute("select max(Count) from Chen.output_table").fetchall()[0][0]
    qc = eng.execute("select count(*) from ApplicationData.raw_data")
    count = qc.fetchall()[0][0]
    if not maxCount or count > maxCount:
        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        for heading in ['lot_size_sqft', 'total_building_sqft', 'yr_built', 'bedrooms', 'total_rooms', 'bath_total', 'final_value']:
            eng.execute(update_statement(heading, count, timestamp))
if __name__ == "__main__":

    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))
