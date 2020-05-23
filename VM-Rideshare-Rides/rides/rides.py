import re
import os
import ast
import requests
import json
import csv
import collections
from flask_api import status
from datetime import datetime
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError
from flask import Flask, jsonify, request, abort,redirect, url_for, session, Response, _request_ctx_stack

#FLASK APP
app = Flask(__name__)
app.config['SECRET_KEY'] = "Ride Share api"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
#FLASK SQLALCHEMY DATABASE
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///rides.db'

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

#RIDES TABLE
class Rides(db.Model):
    __tablename__ = "rides"
    ride_id = db.Column('ride_id', db.Integer, primary_key=True)
    created_by = db.Column('created_by', db.String(100), nullable=False)
    timestamp = db.Column('timestamp', db.String(100), nullable=False)
    source = db.Column('source', db.Integer, nullable=False)
    destination = db.Column('destination', db.Integer, nullable=False)
    joined_users = db.Column('joined_users',db.String(1000),nullable=False)

    def __init__(self, created_by, timestamp, source, destination,joined_users):
        self.created_by = created_by
        self.timestamp = timestamp
        self.source = source
        self.destination = destination
        self.joined_users=joined_users

#COUNT TABLE TO RECORD CALLS MADE TO RIDES
class Count(db.Model):
    __tablename__ = 'count'
    base_count = db.Column('count_id', db.Integer, primary_key=True)
    rides_count = db.Column('rides_count', db.Integer, nullable=False)
    def __init__(self, rides_count):
        self.rides_count = rides_count

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

#ADD RIDE
@app.route('/api/v1/rides',methods = ['POST'])
def AddRide():
    if(request.method=='POST'):
        #INCREMENT COUNT OF RIDES CALL
        quer_res = db.session.query(Count).filter_by(base_count = 1).all()
        rideC = quer_res[0].rides_count
        db.session.query(Count).filter_by(base_count = 1).update(dict(rides_count=rideC+1))
        #COMMIT CHANGES
        db.session.commit()
        #GET JSON DATA SENT IN BODY
        req = request.get_json()
        #CHECK IF SOURCE AND DESTINATION ARE WELL WITHIN THE DEFINE LOCATION BOUNDARIES
        if(int(req['source']) not in range(1,199) or int(req['destination']) not in range(1,199)):
            #IF NOT RETURN BAD REQUEST
            return Response(status=status.HTTP_400_BAD_REQUEST)
        uname = req['created_by']
        #CHECK IF USER TRYING TO CREATE THE RIDE EXISTS
        checkuser = {'table':'Users', 'method':'existence', 'where': uname}
        #SEND REQUEST TO USER GET CHECK WHETHER USER EXISTS
        #CROSS ORIGIN SET
        Exists = requests.get('http://'+dns_ALB+'/api/v1/users', headers = {'Content-Type': 'application/json', 'Accept':'application/json', 'origin':'34.194.83.184'})
        #GET CURRENT TIME
        currtime = datetime.now().strftime('%d-%m-%Y:%S-%M-%H')
        #IF USER DOESNOT EXIST , SAY BAD REQUEST
        if(Exists.status_code == 204):
            return Response(status=400)
        #IF TIMESTAMP FOR RIDE IS IN FUTURE AND IS IN CORRECT PATTERN; CREATE RIDE
        elif(uname in ast.literal_eval(Exists.text) and (ddformat.match(req['timestamp']) is not None) and (currtime < req['timestamp'])):
            addride = {"table" : "Ride" , "insert": req}
            #SEND REQUEST TO DB WRITE
            response = (requests.post(writeURL, data = json.dumps(addride), headers = headers)).json()
            return Response(status=response['status_code'])
        else:
            #ELSE BAD REQUEST
            return Response(status=status.HTTP_400_BAD_REQUEST)
    return Response(status=status.HTTP_405_METHOD_NOT_ALLOWED)


#FETCH RIDES : LISTS ALL RIDES B/W ANY VALID SOURCE AND DESTINATION
@app.route('/api/v1/rides',methods = ['GET'])
def FetchRides():
    if(request.method=='GET'):
        #INCREMENT COUNT OF RIDES CALL
        quer_res = db.session.query(Count).filter_by(base_count = 1).all()
        rideC = quer_res[0].rides_count
        db.session.query(Count).filter_by(base_count = 1).update(dict(rides_count=rideC+1))
        #COMMIT CHANGES
        db.session.commit()
        #GET SOURCE AND DESTINATION, SENT IN URL
        source = int(request.args.get('source'))
        destination = int(request.args.get('destination'))
        #CHECK IF SOURCE AND DESTINATION ARE VALID
        if(source == None or destination == None or not(source in location) or not(destination in location)):
            #ELSE NO CONTENT
            return Response(status=status.HTTP_204_NO_CONTENT)
        #GET CURRENT TIME
        currtime = datetime.now().strftime('%d-%m-%Y:%S-%M-%H')
        queryride = {'table': 'Rides', 'method':'query', "columns" : ['timestamp', 'source', 'destination']
                       , "where" : [currtime, source, destination]}
        #SENDING REQUEST TO DB READ TO READ
        response = (requests.post(readURL, data = json.dumps(queryride), headers = headers)).json()
        #IF RESPONSE IS NOT EMPTY, RETURN LIST
        if(len(response['Result'])>0):
            return jsonify(response['Result'])
        #RETURN NO CONTENT
        return Response(status=status.HTTP_204_NO_CONTENT)
    return Response(status=status.HTTP_405_METHOD_NOT_ALLOWED)


#LIST RIDES: LISTS RIDE DETAILS FOR A GIVEN RIDE ID
@app.route('/api/v1/rides/<rideId>',methods=['GET'])
def listrides(rideId):
    if(request.method=='GET'):
        #INCREMENT COUNT OF RIDES CALL
        quer_res = db.session.query(Count).filter_by(base_count = 1).all()
        rideC = quer_res[0].rides_count
        db.session.query(Count).filter_by(base_count = 1).update(dict(rides_count=rideC+1))
        #COMMIT CHANGES
        db.session.commit()

        checkride = {'table':'Rides', 'method':'existence', 'where': int(rideId)}
        #CHECK IF THAT RIDE ID EXIST
        Exists = (requests.post(readURL, data = json.dumps(checkride), headers = headers)).json()
        if(Exists['Exists']):
            #IF EXISTS THEN LIST DETAILS
            getride={'table':'Rides', 'method': 'list_details','where': int(rideId)}
            #SEND REQUEST TO GET DETAILS OF THE RIDE
            response = (requests.post(readURL, data = json.dumps(getride), headers = headers)).json()
            return jsonify(response['Result'][0])
        else:
            #ELSE RIDE DOESNOT EXIST
            return Response(status=status.HTTP_204_NO_CONTENT)
    return Response(status=status.HTTP_405_METHOD_NOT_ALLOWED)

#JOIN AN EXISTING RIDE : A DIFFERENT USER CAN JOIN A RIDE CREATED BY ANOTHER USER
@app.route('/api/v1/rides/<rideId>',methods=['POST'])
def joinride(rideId):
    if(request.method=='POST'):
        #INCREMENT COUNT OF RIDES CALLS
        quer_res = db.session.query(Count).filter_by(base_count = 1).all()
        rideC = quer_res[0].rides_count
        db.session.query(Count).filter_by(base_count = 1).update(dict(rides_count=rideC+1))
        #COMMIT CHANGES
        db.session.commit()
        #GET USER NAME SENT IN BODY
        req = request.get_json()
        #GET USER LIST TO CHECK IF USER EXISTS
        Existuser = requests.get('http://'+dns_ALB+'/api/v1/users', headers = {'Content-Type': 'application/json', 'Accept':'application/json', 'origin':'34.194.83.184'})
        checkride = {'table':'Rides', 'method':'existence', 'where': int(rideId)}
        #CHECK IF RIDE ID EXISTS 
        Existride = (requests.post(readURL, data = json.dumps(checkride), headers = headers)).json()
        #IF USER DOES NOT EXIST
        if(Existuser.status_code==204):
            return Response(status=204)
        #IF VALID, USER CAN JOIN RIDE
        elif(req['username'] in ast.literal_eval(Existuser.text) and Existride['Exists']):
            joinride={'table':'Rides', 'method':'join','where': [req['username'], int(rideId)]}
            #SEND REQUEST TO JOIN RIDE
            response = (requests.post(writeURL, data = json.dumps(joinride), headers = headers)).json()
            return Response(status=response['status_code'])
        else:
            #ELSE NOT VALID, RETURN NO CONTENT
            return Response(status=status.HTTP_204_NO_CONTENT)
    return Response(status=status.HTTP_405_METHOD_NOT_ALLOWED)


#DELETE RIDES
@app.route('/api/v1/rides/<rideId>', methods = ['DELETE'])
def DeleteRide(rideId):
    if request.method=='DELETE':
        #INCREMENT COUNT OF RIDE CALL
        quer_res = db.session.query(Count).filter_by(base_count = 1).all()
        rideC = quer_res[0].rides_count
        db.session.query(Count).filter_by(base_count = 1).update(dict(rides_count=rideC+1))
        #COMMIT CHANGES
        db.session.commit()
        checkride = {'table':'Rides', 'method':'existence', 'where': int(rideId)}
        #CHECK IF RIDE EXISTS BEFORE DELETION
        Exists = (requests.post(readURL, data = json.dumps(checkride), headers = headers)).json()
        #IF RIDE EXISTS, PROCEED WITH DELETION
        if(Exists['Exists']):
            delride = {"table" : "Rides" , "delete" : rideId}
            #SEND DELETE REQUEST
            response = (requests.post(writeURL, data = json.dumps(delride), headers = headers)).json()
            return Response(status=response['status_code'])
        else:
            #ELSE BAD REQUEST
            return Response(status=status.HTTP_400_BAD_REQUEST)
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
        rideC = quer_res[0].rides_count
        db.session.query(Count).filter_by(base_count = 1).update(dict(rides_count=rideC+1))
        #COMMIT CHANGES
        db.session.commit()
    return response

#COUNT API : RETURNS COUNT OF RIDES CALLS MADE
@app.route('/api/v1/_count', methods = ['GET'])
def Count_Call():
    if(request.method=='GET'):
        #GETTING COUNT
        quer_res = db.session.query(Count).filter_by(base_count = 1).all()
        res = []
        #RETURN COUNT AS A LIST
        res.append(quer_res[0].rides_count)
        return jsonify(res)
    return Response(status=status.HTTP_405_METHOD_NOT_ALLOWED)

#RESET COUNT
@app.route('/api/v1/_count', methods = ['DELETE'])
def Reset_Count():
    if(request.method=='DELETE'):
        #UPDATE COUNT TO 0
        db.session.query(Count).filter_by(base_count = 1).update(dict(rides_count=0))
        db.session.commit()
        #ONCE DONE, RETURN 200 SUCCESS
        return Response(status=status.HTTP_200_OK)
    return Response(status=status.HTTP_405_METHOD_NOT_ALLOWED)

#GET NUMBER OF RIDES
@app.route('/api/v1/rides/count', methods = ['GET'])
def Rides_Count():
    if(request.method=='GET'):
        #INCREMENT COUNT OF RIDES CALL
        quer_res = db.session.query(Count).filter_by(base_count = 1).all()
        rideC = quer_res[0].rides_count
        db.session.query(Count).filter_by(base_count = 1).update(dict(rides_count=rideC+1))
        #COMMIT CHANGES
        db.session.commit()

        getrides={'table':'Rides', 'method': 'list_rides'}
        #SEND REQUEST TO DB READ TO FETCH LIST OF RIDES
        response = (requests.post(readURL, data = json.dumps(getrides), headers = headers)).json()
        res = []
        #RETURN LENGTH OF LIST RECEIVED AS RESPONSE
        res.append(len(response['Result']))
        return jsonify(res)
    return Response(status=status.HTTP_405_METHOD_NOT_ALLOWED)


if(__name__ == '__main__'):
    #RUN FLASK APP
    app.run(debug=True, host='0.0.0.0')