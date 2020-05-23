import re
import os
import ast
import json
import csv
import collections
from flask_api import status
from datetime import datetime
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError
from flask import Flask, jsonify, request, abort,redirect, url_for, session, Response, _request_ctx_stack
import requests

#FLASK APP
app = Flask(__name__)
app.config['SECRET_KEY'] = "Ride Share api"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
#FLASK SQLALCHEMY DATABASE
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///users.db'

#DEFINE A MAP FOR LOCATION TO IT'S LOCATION ID 
location = collections.defaultdict(str)
with open('AreaNameEnum.csv', 'r') as fin:
    dictr = csv.DictReader(fin)
    for line in dictr:
        location[int(line['Area No'])] = line['Area Name']

session_options = {'autocommit':False, 'autoflush':False}
#FLASK SQLALCHEMY
db = SQLAlchemy(app)

# DNS OF APPLICATION LOADBALANCER
dns_ALB = 'path-based-alb-973289095.us-east-1.elb.amazonaws.com'
# IP OF ORCHESTRATOR
orch_ip = 'http://54.87.137.56:80'

#PATTERN TO CHECK IF PASSWORD IS IN SHA1 ENCRYPTION FORMAT
SHA1 = re.compile(r'\b[a-fA-F0-9]{40}\b')
#PATTERN TO CHECK IF TIMESTAMP IS IN CORRECT FORMAT
ddformat = re.compile(r'\d{2}-\d{2}-\d{4}:\d{2}-\d{2}-\d{2}')
#HEADERS TO SEND REQUEST
headers = {'Content-Type': 'application/json', 'Accept':'application/json'}
#URL FOR DB WRITE ON ORCHESTRATOR
writeURL = orch_ip+'/api/v1/db/write'
#URL FOR DB READ ON ORCHESTRATOR
readURL = orch_ip+'/api/v1/db/read'
#METHODS
METHODS = ['GET', 'PUT', 'POST', 'DELETE', 'PATCH', 'COPY', 'HEAD', 'OPTIONS', 'LINK'
                , 'UNLINK', 'PURGE', 'LOCK', 'UNLOCK', 'PROPFIND', 'VIEW']

#USER TABLE
class Users(db.Model):
    __tablename__ = 'users'
    username = db.Column('username', db.String(100), primary_key=True)
    password = db.Column('password', db.String(40), nullable=False)
    def __init__(self, username, password):
        self.username = username
        self.password = password

#COUNT TABLE TO RECORD CALLS MADE TO USER
class Count(db.Model):
    __tablename__ = 'count'
    base_count = db.Column('count_id', db.Integer, primary_key=True)
    users_count = db.Column('users_count', db.Integer, nullable=False)
    def __init__(self, users_count):
        self.users_count = users_count

#CREATE SCHEMA
db.create_all()

#INITIALISE COUNT TO 0
quer_count = db.session.query(Count).filter_by(base_count=1)
if(quer_count.scalar() is None):
    baseC = Count(0)
    db.session.add(baseC)
    db.session.commit()

#HealthCheck ALB
@app.route('/api/v1/healthcheck', methods = ['GET'])
def healthcheck():
    return Response(status=status.HTTP_200_OK)

#ADD USER API CALLED AT PUT
@app.route('/api/v1/users' , methods = ['PUT'])
def AddUser():
    if(request.method=='PUT'):
        #INCREASE COUNT OF USER CALLS
        quer_res = db.session.query(Count).filter_by(base_count = 1).all()
        userC = quer_res[0].users_count
        db.session.query(Count).filter_by(base_count = 1).update(dict(users_count=userC+1))
        #COMMIT CHANGES
        db.session.commit()
        #GET JSON DATA SENT IN BODY
        req = request.get_json()
        #CHECK IF PASSWORD MATCHES REQUIRED PATTERN
        try:
            if(not(re.match(SHA1, req['password']))):
                raise ValueError
        #ELSE RAISE VALUE ERROR WITH RESPONSE AS BAD REQUEST
        except ValueError:
            return Response(status=400)
        #CREATE A REQUEST TO BE SENT TO DB WRITE
        adduser = {"table" : "Users", "insert" : req }
        #SEND REQUEST TO DB WRITE
        response = (requests.post(writeURL, data = json.dumps(adduser), headers = headers))
        #PARSE RESPONSE
        response = response.json()
        #RETURN RESPONSE STATUS
        return Response(status=response['status_code'])
    #RESPOND FOR WRONG METHOD USED
    return Response(status=405)

#REMOVE USER
@app.route('/api/v1/users/<uname>', methods = ['DELETE'])
def RemoveUser(uname):
    if request.method=='DELETE':
        #INCREMENT COUNT OF USER CALLS
        quer_res = db.session.query(Count).filter_by(base_count = 1).all()
        userC = quer_res[0].users_count
        db.session.query(Count).filter_by(base_count = 1).update(dict(users_count=userC+1))
        #COMMIT CHANGES
        db.session.commit()
        #CHECK WHETHER USER ALREADY REQUESTS
        checkuser = {'table':'Users', 'method':'existence', 'where': uname}
        #SEND REQUEST TO CHECK FOR EXISTENCE
        Exists = (requests.post(readURL, data = json.dumps(checkuser), headers = headers)).json()
        #IF EXISTS SEND REQUEST TO DELETE USER
        if(Exists['Exists']):
            #SEND REQUEST TO DB WRITE
            removeuser = {"table" : "Users" , "delete" : uname}
            response = (requests.post(writeURL, data = json.dumps(removeuser), headers = headers)).json()
            #RETURN RESPONSE STATUS
            return Response(status=response['status_code'])
        else:
            #IF USER DOESNOT EXIST, BAD REQUEST
            return Response(status=status.HTTP_400_BAD_REQUEST)
    return Response(status=status.HTTP_405_METHOD_NOT_ALLOWED)

#List All Users
@app.route('/api/v1/users',methods=['GET'])
def listusers():
    if(request.method=='GET'):
        #INCREMENT COUNT OF USER COUNT
        quer_res = db.session.query(Count).filter_by(base_count = 1).all()
        userC = quer_res[0].users_count
        db.session.query(Count).filter_by(base_count = 1).update(dict(users_count=userC+1))
        #COMMIT CHANGES
        db.session.commit()
        #SEND REQUEST TO DB READ TO GET USER LIST
        getusers={'table':'Users', 'method': 'list_users'}
        response = (requests.post(readURL, data = json.dumps(getusers), headers = headers)).json()
        #IF THERE ARE ANY USERS, RETURN THE LIST
        if(len(response['Result'])>0):
            return jsonify(response['Result'])
        else:
            #IF NO USERS, RETURN NO CONTENT
            return Response(status=status.HTTP_204_NO_CONTENT)
    return Response(status=status.HTTP_405_METHOD_NOT_ALLOWED)

#FUNCTION IS TRIGGERED AFTER A REQUEST IS RECIEVED IS SERVICES BY FLASK
@app.after_request
def after_request_callback(response):
    #POPPING THE URL OF API THAT WAS LAST REQUESTED TO FROM FLASK REQUEST STACK
    path = _request_ctx_stack.top.request.url
    #INCREMENT COUNT FOR USER CALL FOR WRONG METHOD
    #AS FLASK THROWS ERROR ON BAD REQUEST
    if(response.status == '405 METHOD NOT ALLOWED' and '/api/v1/_count' not in path and '/db/clear' not in path):
        quer_res = db.session.query(Count).filter_by(base_count = 1).all()
        userC = quer_res[0].users_count
        db.session.query(Count).filter_by(base_count = 1).update(dict(users_count=userC+1))
        #COMMIT CHANGES
        db.session.commit()
    return response

#COUNT API : RETURNS COUNT OF USER CALLS MADE
@app.route('/api/v1/_count', methods = ['GET'])
def Count_Call():
    if(request.method=='GET'):
        #GETTING COUNT
        quer_res = db.session.query(Count).filter_by(base_count = 1).all()
        res = []
        #RETURN COUNT AS A LIST
        res.append(quer_res[0].users_count)
        return jsonify(res)
    return Response(status=status.HTTP_405_METHOD_NOT_ALLOWED)

#RESET COUNT
@app.route('/api/v1/_count', methods = ['DELETE'])
def Reset_Count():
    if(request.method=='DELETE'):
        #UPDATE COUNT TO 0
        db.session.query(Count).filter_by(base_count = 1).update(dict(users_count=0))
        db.session.commit()
        #ONCE DONE, RETURN 200 SUCCESS
        return Response(status=status.HTTP_200_OK)
    return Response(status=status.HTTP_405_METHOD_NOT_ALLOWED)

if(__name__ == '__main__'):
    #RUN FLASK APP
    app.run(debug=True, host='0.0.0.0')