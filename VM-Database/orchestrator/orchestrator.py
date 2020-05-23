import re
import os
import ast
import requests
import json
import csv
import random
import collections
from flask_api import status
from datetime import datetime
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError
from flask import Flask, jsonify, request, abort,redirect, url_for, session, Response
#RMQP RELATED IMPORTS
import pika
import uuid
#DockerSDK AND SCALING RELATED IMPORTS
import docker
import sched, time, threading, psutil, math
import subprocess
import multiprocessing
import logging
#ZOOKEEPER (KAZOO) IMPORTS 
from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.client import EventType
from kazoo.client import KeeperState


#INITIAL DELAY TO WAIT FOR OTHER SERVICES TO START 
time.sleep(15)
#VARIABLE THAT MARKS THE FIRST READ REQUEST RECEIVED
first_req = True
#VARIABLE THAT MARKS WHETHER THE METHOD WAS TRIGGERED BECAUSE OF SCALING
scale_req = False
#INIT FOR DIFFERENCE OF NUM OF SLAVES RUNNING AND NUM OF SLAVES REQUIRED
diff_slaves = -9999
#VARIABLE THAT MARKS WHETHER ITS A CRASH WORKER OPERATION
kill_worker = False

#GETTING A HANDLE TO DOCKER SDK CLIENT
client = docker.DockerClient(base_url = 'unix://var/run/docker.sock')

#FLASK APP
app = Flask(__name__)
app.config['SECRET_KEY'] = "Ride Share api"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///orchestrator.db'

#FLASK HEADERS
headers = {'Content-Type': 'application/json', 'Accept':'application/json'}
session_options = {'autocommit':False, 'autoflush':False}
#FLASK SQLALCHEMY
db = SQLAlchemy(app)

#SINGLE ROW COUNT TABLE WHICH STORES NUM OF READ REQUESTS RECEIVED IN THE 2 MINUTE INTERVAL
class Count(db.Model):
    __tablename__ = 'count'
    base_count = db.Column('count_id', db.Integer, primary_key=True)
    read_count = db.Column('read_count', db.Integer, nullable=False)
    def __init__(self, read_count):
        self.read_count = read_count

#CREATE TABLE
db.create_all()

#INITIALISE THE ONLY ROW OF COUNT TABLE
quer_count = db.session.query(Count).filter_by(base_count=1)
if(quer_count.scalar() is None):
    baseC = Count(1)
    db.session.add(baseC)
    db.session.commit()


#GENERATES A RANDOM INTEGER USED FOR WORKER NAME
def gen_id():
        return random.randint(1,9999999999)

#RETURNS PID OF THE CONTAINER NAME RECEIVED AS ARGUMENT
def get_pid(con_name):
        #USING SUBPROCESS MODULE, WE CAN RUN SYSTEM COMMANDS AND RECEIVE OUTPUTS
        #DOCKER INSPECT FOR THE CONTAINER NAME WILL RETURN THE PID OF THE WORKER
        pid = subprocess.check_output('docker inspect --format \'{{.State.Pid}}\' '+ con_name,
                shell=True).decode('utf-8')[:-1]
        #RETURN PID AS INT
        return int(pid)

#RETURNS A LIST WITH [WORKER NAME, WORKER PID] AS SUBLISTS SORTED BASED ON WORKER PID IN INCREASING ORDER
def inspect():
        #USING DOCKER SDK TO GET A LIST OF ALL RUNNING CONTAINERS
        conlist = client.containers.list()
        insplist = []
        #ITERATING THROUGH THE LIST AND PROCESSING ONLY THE WORKER CONTAINERS
        for con in conlist:
                if('worker' in con.name):
                        insplist.append([con.name, get_pid(con.name)])
        #SORT
        insplist.sort(key=lambda x: x[1])
        return insplist

#KAZOO CONNECTION LISTENER, PRINTS THE STATE OF THE CONNECTION
def listener(state):
        if(state == KazooState.LOST):
                print("Session Lost")
        elif(state == KazooState.SUSPENDED):
                print("Session Suspended")
        else:
                print("Server Up")

#CONNECTING TO ZOOKEEPER SERVICE
zk = KazooClient(hosts='zookeeper:2181')
#ADD LISTENER
zk.add_listener(listener)
#START ZOOKEEPER SERVICE
zk.start()

#CHECK IF /election ROOT NODE EXISTS ALREADY
#ELSE CREATE AND INITIALISE WITH A HIGH VALUE WHICH WILL BE HELPFUL DURING ELECTION 
if(zk.exists('/election')== None):
        #CREATE PERSISTENT NODE
        zk.create('/election',bytes("99999999999", 'utf-8'))
else:
        #IF ALREADY EXISTS, INITIALISE AGAIN FOR THIS ORCHESTRATOR SESSION
        zk.set('/election', bytes("99999999999", 'utf-8'))

#WORKER NAME
mname = "worker_"+str(gen_id())
#STARTING A NEW WORKER CONTAINER INITIALLY
#ATTACHING SHARED VOLUME
#CREATE AND EXTERNAL VOLUME NAMED 'Shared'
mcon = client.containers.run(image='worker:latest',name=mname, network='rabbitmq', environment={"ENV_VAR":mname},
       volumes={'/home/ubuntu/worker/worker.py':{'bind':'/src/app.py', 'mode':'ro'}, '/usr/bin/docker': {'bind':'/usr/bin/docker', 'mode': 'ro'},
        '/var/run/docker.sock' : {'bind': '/var/run/docker.sock', 'mode' : 'ro'}, 'Shared' : {'bind':'/Sharedvolume', 'mode': 'rw'}}, detach=True)

#WORKER NAME
sname = "worker_"+str(gen_id())
#STARTING A NEW WORKER CONTAINER INITIALLY USING A PRE-CREATED WORKER IMAGE
#ATTACHING SHARED VOLUME
scon = client.containers.run(image='worker:latest',name=sname, network='rabbitmq', environment={"ENV_VAR":sname},
       volumes={'/home/ubuntu/worker/worker.py':{'bind':'/src/app.py', 'mode':'ro'}, '/usr/bin/docker': {'bind':'/usr/bin/docker', 'mode': 'ro'},
        '/var/run/docker.sock' : {'bind': '/var/run/docker.sock', 'mode' : 'ro'}, 'Shared' : {'bind':'/Sharedvolume', 'mode': 'rw'}}, detach=True)

#CALLBACK FUNCTION FOR WATCH SET ON WORKERS FOR FAULT TOLERANCE
def worker_killed(event):
        global scale_req
        #CHECK IF FUNCTION WAS TRIGGERED DURING SCALING: DO NOTHING
        if(scale_req):
                pass
        #TRIGGERED BECAUSE OF FAULT: SPAWN A NEW WORKER 
        elif(scale_req==False):
                #NEW WORKER NAME
                newSname = "worker_"+str(gen_id())
                #CREATE AND START NEW WORKER CONATINER
                newScon = client.containers.run(image='worker:latest',name=newSname, network='rabbitmq', environment={"ENV_VAR":newSname},
                volumes={'/home/ubuntu/worker/worker.py':{'bind':'/src/app.py', 'mode':'ro'}, '/usr/bin/docker': {'bind':'/usr/bin/docker', 'mode': 'ro'},
                        '/var/run/docker.sock' : {'bind': '/var/run/docker.sock', 'mode' : 'ro'}, 'Shared' : {'bind':'/Sharedvolume', 'mode': 'rw'}}, detach=True)


#SETTING A WATCH ON THE WORKER i.e CHILDREN OF THE ROOT NODE
#EVENT : CHILD
#TRIGGERED WHEN A NEW WORKER IS CREATED OR A WORKER STOPS
@zk.ChildrenWatch('/election', send_event=True)
def watch_children(children, event):
        #CHILDREN CONTAINS NAME OF ALL CURRENT RUNNING WORKER CONTAINER i.e CHILDREN NODES
        for w in children:
                #NAME OF THE WORKER ZNODE 
                w_path = '/election/'+ w
                #SET A WATCH ON THE WORKER ZNODE
                znode = zk.get(w_path, watch=worker_killed)

#Read RPC CLASS FOR READ QUEUE REQUESTS
class readRPC_Client(object):
    def __init__(self):
        #CONNECT TO PIKA (RABBITMQ SERVER)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq-server', heartbeat=0))
        #DEFINE CONNECTION CHANNEL
        self.channel = self.connection.channel()
        #CREATE RESPONSE QUEUE
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue
        #CONSUME FROM THE RESPONSE QUEUE ONCE THE RESPONSE FROM WORKER ARRIVES
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

        #CALLBACK FUNCTION ONCE RESPONSE IS RECEIVED
    def on_response(self, ch, method, props, body):
        #CORRELATION ID IS USED TO MAP REQUEST TO IT'S RESPONSE
        if self.corr_id == props.correlation_id:
            self.response = body

        #CALL FUNCTION RECEIVES REQUESTS AND PUBLISHES THEM ON THE QUEUE 
    def call(self, n):
        self.response = None
        #SET CORRELATION ID
        self.corr_id = str(uuid.uuid4())
        #PUBLISH REQUEST TO THE QUEUE
        self.channel.basic_publish(
            exchange='rpc',
            routing_key='read_queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=str(n))
        while self.response is None:
            self.connection.process_data_events()
        return self.response

#CREATE AN OBJECT TO READ RPC
readRPC = readRPC_Client()

#Write RPC CLASS FOR READ QUEUE REQUESTS
class writeRPC_Client(object):
    def __init__(self):
        #CONNECT TO PIKA (RABBITMQ SERVER)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq-server', heartbeat=0))
        #DEFINE CONNECTION CHANNEL
        self.channel = self.connection.channel()
        #CREATE RESPONSE QUEUE
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue
        #CONSUME FROM THE RESPONSE QUEUE ONCE THE RESPONSE FROM WORKER ARRIVES
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)
        #CALLBACK FUNCTION ONCE RESPONSE IS RECEIVED
    def on_response(self, ch, method, props, body):
        #CORRELATION ID IS USED TO MAP REQUEST TO IT'S RESPONSE
        if self.corr_id == props.correlation_id:
            self.response = body

        #CALL FUNCTION RECEIVES REQUESTS AND PUBLISHES THEM ON THE QUEUE 
    def call(self, n):
        self.response = None
        #SET CORRELATION ID
        self.corr_id = str(uuid.uuid4())
        #PUBLISH REQUEST TO THE QUEUE
        self.channel.basic_publish(
            exchange='rpc',
            routing_key='write_queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=str(n))
        while self.response is None:
            self.connection.process_data_events()
        return self.response

#CREATE AN OBJECT TO WRITE RPC
writeRPC = writeRPC_Client()

#NOT NEEDED
def scale_kill(event):
        pass

#RESPONSIBLE FOR SCALING 
def scale():
        #THREADING THE SACLING FUNCTION THAT IS CALLED EVERY 2 MINUTES
        threading.Timer(120.0, scale).start()
        global scale_req
        #SET TO TRUE AS IT IS SCALING OPERATION
        scale_req = True
        #global diff_slaves
        #READ NUM OF REQUESTS FROM DB
        quer_res = db.session.query(Count).filter_by(base_count=1).all()
        #READ COUNT
        readC = (quer_res[0].read_count) #/2
        #RESET READ COUNT TO 0
        db.session.query(Count).filter_by(base_count=1).update(dict(read_count=0))
        db.session.commit()
        #NUMBER OF SLAVES NEEDED BASED ON THE NUMBER OF REQUESTS MADE
        numSlavesNeeded = math.ceil(readC/20)
        #IF 0 READ REQUESTS MADE STILL ONE SLAVE IS NEEDED
        if(numSlavesNeeded==0):
                numSlavesNeeded=1
        #GET SORTED LIST OF WORKERS
        conlist = inspect()
        #LIST WITH ONLY SLAVES
        conlist = conlist[1:]
        count_slaves = len(conlist)
        #HOW MUCH IT DIFFERS
        diff_slaves = count_slaves - numSlavesNeeded
        logging.warning("SCALING:")
        logging.warning(diff_slaves)
        #IF NUMBER OF WORKERS NEEDED IS MORE THAN WHAT IS REQUIRED
        if(diff_slaves>0):
                #ITERATE IN REVERSE ORDER AS MAX PID SLAVES NEED TO BE DELETED
                for i in conlist[-diff_slaves:]:
                        #WORKER ZNODE NAME
                        worker_path = '/election/'+i[0]
                        #GET WORKER CONTAINER
                        con = client.containers.get(i[0])
                        #STOP THE CONTAINER
                        subprocess.run(["docker", "kill", i[0]])
        #IF NUMBER OF WORKERS IS LESS THAN WHAT IS RQEQUIRED
        elif(diff_slaves<0):
                scale_req = False
                #SPAWN THE NUMBER OF WORKERS REQUIRED
                for j in range(abs(diff_slaves)):
                        newSname = "worker_"+str(gen_id())
                        newScon = client.containers.run(image='worker:latest',name=newSname, network='rabbitmq', environment={"ENV_VAR":newSname},
                        volumes={'/home/ubuntu/worker/worker.py':{'bind':'/src/app.py', 'mode':'ro'}, '/usr/bin/docker': {'bind':'/usr/bin/docker', 'mode': 'ro'},
                        '/var/run/docker.sock' : {'bind': '/var/run/docker.sock', 'mode' : 'ro'}, 'Shared' : {'bind':'/Sharedvolume', 'mode': 'rw'}}, detach=True)

#DB WRITE API
@app.route('/api/v1/db/write', methods = ['POST'])
def DBWrite():
    if(request.method == 'POST'):
        #DESTRINGIFY JSON
        Request = (request.get_data()).decode()
        Request = ast.literal_eval(Request)
        #SEND REQUEST TO WRITE RPC
        response = writeRPC.call(Request)
        response = ast.literal_eval(response.decode())
        #RETURN RESPONSE
        return response

#DB READ API
@app.route('/api/v1/db/read', methods = ['POST'])
def DBRead():
        global first_req
        if(request.method=='POST'):
                #DESTRINGIFY JSON
                Request = (request.get_data()).decode()
                Request = ast.literal_eval(Request)
                #UPDATE REQUEST REQUEST COUNT
                quer_res = db.session.query(Count).filter_by(base_count = 1).all()
                readC = quer_res[0].read_count
                db.session.query(Count).filter_by(base_count = 1).update(dict(read_count=readC+1))
                db.session.commit()
                response = readRPC.call(Request)
                #IN CASE OF FIRST READ REQUEST
                if(first_req):
                        #UPDATE COUNT TO 1
                        db.session.query(Count).filter_by(base_count = 1).update(dict(read_count=1))
                        db.session.commit()
                        first_req = False
                        #START SCALING
                        scale()
                response = ast.literal_eval(response.decode())
                return response

#DB CLEAR API
@app.route('/api/v1/db/clear', methods=['POST'])
def DBClear():
        if(request.method=='POST'):
                Request = {"clearDB": 1}
                #WRITE REQUEST SO PUBLISH TO WRITE RPC
                response = writeRPC.call(Request)
                response = ast.literal_eval(response.decode())
                return Response(status=status.HTTP_200_OK)
        return Response(status=status.HTTP_405_METHOD_NOT_ALLOWED)

#CRASH API
@app.route('/api/v1/crash/master', methods=['POST'])
def crashMaster():
        global scale_req
        scale_req = False
        #GET SORTED WORKER LIST
        insplist = inspect()
        #FIRST ONE IS MASTER
        mastercon = client.containers.get(insplist[0][0])
        mastercon.kill()
        #ONCE MASTER IS KILLED, INITIALISE ROOT NODE FOR ELECTION
        zk.set('/election', bytes("99999999999", 'utf-8'))
        return jsonify([insplist[0][1]]),200

@app.route('/api/v1/crash/slave', methods=['POST'])
def crashSlave():
        global scale_req
        scale_req = False
        #GET LIST
        insplist = inspect()
        #LAST SLAVE WILL HAVE MAX PID
        slavecon = client.containers.get(insplist[len(insplist)-1][0])
        slavecon.kill()
        return jsonify([insplist[len(insplist)-1][1]]),200

#wORKER LIST
@app.route('/api/v1/worker/list', methods=['GET'])
def listWorkers():
        #GET SORTED LIST
        insplist = inspect()
        reslist = []
        for con in insplist:
                #APPEND WORKER NAME
                reslist.append(con[1])
        return jsonify(reslist),200

if(__name__ == '__main__'):
        #START FLASK APP
    app.run(debug=True,host='0.0.0.0', use_reloader=False)