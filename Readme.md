# Flask Celery Redis Server for Tracking MySQL Table Update

## install redis on ubuntu

- download and unzip redis file
- cd to the folder and run make
- run make test
- run make install
- run redis-server

## Celery
- setup python environment
- pip install -r requirements.txt
- run "celery -A app.celery -B"

## run the app

- configure .env file
- python app.py
- visit "localhost:5000/updateStats"
