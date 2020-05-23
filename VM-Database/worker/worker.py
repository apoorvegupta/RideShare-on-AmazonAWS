import os
import ast
import requests
import json
from flask_api import status
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError
from flask import Flask, jsonify, request, abort,redirect, url_for, session, Response
#AMQP RELATED IMPORTS
import pika
import uuid
#DATABASE RELATED IMPORTS
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, String, Integer, Date, Table, ForeignKey
from sqlalchemy.orm import relationship
import subprocess
import logging
#ZOOKEEPER KAZOO IMPORTS
from kazoo.client import KazooClient
from kazoo.client import KazooState
import multiprocessing
import  time
#LOGGING
logging.basicConfig(level=logging.WARNING)

#GET WORKER NAME USING ENVIRONMENT VARIABLE
worker_name = subprocess.check_output("printenv ENV_VAR",shell=True).decode('utf-8')[:-1]
#GET WORKER PID
worker_pid = subprocess.check_output('/usr/bin/docker inspect --format \'{{.State.Pid}}\' '+ worker_name,
                shell=True).decode('utf-8')[:-1]

#TO CHECK STATE OF THE ZOOKEEPER CONNECTION
def listener(state):
        if state == KazooState.LOST:
                print("Session Lost")
        elif state == KazooState.SUSPENDED:
                print("Session Suspended")
        else:
                print("Worker Session Up")

#CONNECT TO ZOOKEEPER SERVER
zk = KazooClient(hosts='zookeeper:2181')
#START ZOOKEEPER
zk.start()
#ADD CONNECTION LISTENER
zk.add_listener(listener)
#CREATE EPHEMERAL NODE FOR THE WORKER
zk.create('/election/'+worker_name, bytes(str(worker_pid),'utf-8'), ephemeral=True)

#CONNECT TO WORKER DATABASE
engine = create_engine('sqlite:///database.db')
#START SESSION
Session = sessionmaker(bind=engine)

#CONNECT TO SHARED VOLUME DB
engine1 = create_engine('sqlite:///Sharedvolume/database.db')
Session1 = sessionmaker(bind=engine1)

#DEFINE DATABASE STRUCTURE
Base = declarative_base()

#USER TABLE
class Users(Base):
    __tablename__ = 'users'
    username = Column('username', String(100), primary_key=True)
    password = Column('password', String(40), nullable=False)
    def __init__(self, username, password):
        self.username = username
        self.password = password

#RIDES TABLE
class Rides(Base):
    __tablename__ = "rides"
    ride_id = Column('ride_id', Integer, primary_key=True)
    created_by = Column('created_by', String(100), nullable=False)
    timestamp = Column('timestamp', String(100), nullable=False)
    source = Column('source', Integer, nullable=False)
    destination = Column('destination', Integer, nullable=False)
    joined_users = Column('joined_users', String(1000),nullable=False)

    def __init__(self, created_by, timestamp, source, destination,joined_users):
        self.created_by = created_by
        self.timestamp = timestamp
        self.source = source
        self.destination = destination
        self.joined_users=joined_users

#SET UP TABLES
Base.metadata.create_all(engine)
session = Session()

#SETUP TABLES
Base.metadata.create_all(engine1)
session1 = Session1()

#REPLICATION STEP
#COPY SHARED VOLUME DATABASE TO WORKER CONTAINER
subprocess.run(["cp", "Sharedvolume/database.db", "."], capture_output=True)

#DB WRITE
def DBWrite(Request):
        #IF INSERT REQUEST
        if('insert' in Request):
                try:
                        #IF USER TABLE
                        if(Request['table'] == 'Users'):
                                #CREATE OBJECT OS CLASS
                                obj = Users(Request['insert']['username'], Request['insert']['password'])
                                obj1 = Users(Request['insert']['username'], Request['insert']['password'])
                        #IF RIDES TABLE
                        elif(Request['table'] == 'Ride'):
                                obj= Rides(created_by=Request['insert']['created_by'], timestamp=Request['insert']['timestamp']
                                , source=int(Request['insert']['source']), destination=int(Request['insert']['destination']) , joined_users='')

                                obj1= Rides(created_by=Request['insert']['created_by'], timestamp=Request['insert']['timestamp']
                                , source=int(Request['insert']['source']), destination=int(Request['insert']['destination']) , joined_users='')

                        session.add(obj)
                        session.commit()

                        session1.add(obj1)
                        session1.commit()
                #IN CASE OF WRONG REQUEST ROLLBACK
                except IntegrityError:
                        session.rollback()
                        session1.rollback()
                        #IN CASE OF BAD REQUEST
                        return {'status_code': 400}
                #SUCCESSFULLY CREATED
                return {'status_code': 201}
        elif('delete' in Request):
                if(Request['table'] == 'Users'):
                        #FILTER AND DELETE BY USERNAME
                        session.query(Users).filter_by(username = Request['delete']).delete()
                        session.query(Rides).filter_by(created_by = Request['delete']).delete()
                        session1.query(Users).filter_by(username = Request['delete']).delete()
                        session1.query(Rides).filter_by(created_by = Request['delete']).delete()

                elif(Request['table'] == 'Rides'):
                        #FILTER AND DELETE BY RIDE ID
                        session.query(Rides).filter_by(ride_id = Request['delete']).delete()
                        session1.query(Rides).filter_by(ride_id = Request['delete']).delete()
                session.commit()
                session1.commit()
                #RETURN SUCCESS ONCE DONE
                return {'status_code': 200}
        #JOIN RIDES
        elif(Request['table']=='Rides' and Request['method']=='join'):
                #FIND ENTRY WITH RIDE ID
                quer_res = session.query(Rides).filter_by(ride_id = Request['where'][1]).all()
                if((Request['where'][0] not in quer_res[0].joined_users) and (Request['where'][0] not in quer_res[0].created_by)):
                        #GET RIDE SHARE STRING FROM DB
                        newusersadded = quer_res[0].joined_users+"$#"+Request['where'][0]
                        #UPDATE RIDE SHARE
                        session.query(Rides).filter_by(ride_id=Request['where'][1]).update(dict(joined_users=newusersadded), synchronize_session='fetch')
                        session.commit()
                        #UPDATE RIDE SHARE
                        session1.query(Rides).filter_by(ride_id=Request['where'][1]).update(dict(joined_users=newusersadded), synchronize_session='fetch')
                        session1.commit()
                        return {'status_code': 200}
        #USED FOR ASSIGNMENT 2 AND 3, NOT USED HERE
        elif(Request['table']=='Rides' and Request['method']=='delete_for_user'):
                session.query(Rides).filter_by(created_by = Request['per_delete']).delete()
                session.commit()
                session1.query(Rides).filter_by(created_by = Request['per_delete']).delete()
                session1.commit()
                return {'test':'return'}


#DB READ
def DBRead(Request):
    if(Request['table']=='Users'):
        #CHECK FOR EXISTENCE
        if(Request['method']=='existence'):
            #QUERY USER
            quer_res = session.query(Users).filter_by(username = Request['where'])
            if(quer_res.scalar() is None):
                response = {"Exists": 0}
            else:
                response = {"Exists": 1}
        #LIST USERS
    if(Request['method']=='list_users'):
            #QUERY USERS
            quer_res = session.query(Users).all()
            response_all = []
            #APPEND USERNAME
            for row in quer_res:
                response_all.append(row.username)
            response = {"Result" : response_all}
        #QUERY RIDES
    if(Request['table']=='Rides'):
        if(Request['method']=='query'):
            #FILTER BASED ON SOURCE AND DESTINATION
            quer_res = session.query(Rides).filter_by(source = Request['where'][1]).filter_by(destination = Request['where'][2]).all()
            response_all = []
            for row in quer_res:
                rowd = {}
                #DO THE FOLLOWING IF THE RIDE WAS CREATED FOR THE FUTURE
                #HAS NOT ALREADY EXPIRED
                if row.timestamp > Request['where'][0]:
                    rowd['rideId'] = row.ride_id
                    rowd['username'] = row.created_by
                    rowd['timestamp'] = row.timestamp
                    response_all.append(rowd)
                #RETURN LIST
            response = {"Result" : response_all}
        
        #LIST DETAILS OF RIDE
        if(Request['method']=='list_details'):
            #QUERY RIDE
            quer_res = session.query(Rides).filter_by(ride_id=Request['where'])
            response_all = []
            for row in quer_res:
                rowd = {}
                #APPEND DETAILS OF THE RIDE
                rowd['rideId'] = row.ride_id
                rowd['Created_by'] = row.created_by
                rowd['users']=((row.joined_users).split('$#'))[1:]
                rowd['Timestamp'] = row.timestamp
                rowd['source']=row.source
                rowd['destination']=row.destination
                response_all.append(rowd)
                #RETURN RESPONSE LIST
            response = {"Result" : response_all}

        #CHECK FOR EXISTENCE
        if(Request['method']=='existence'):
            #QUERY RIDE
            quer_res = session.query(Rides).filter_by(ride_id = Request['where'])
            #CHECK IF QUERY RESULT IS EMPTY
            if(quer_res.scalar() is None):
                response = {"Exists": 0}
            else:
                response = {"Exists": 1}
        
        #LIST RIDES
        if(Request['method']=='list_rides'):
            quer_res = session.query(Rides).all()
            response_all = []
            for row in quer_res:
                response_all.append(row.ride_id)
            #RETURN RESPONSE LIST
            response = {"Result" : response_all}

        if(Request['method']=='list_rides'):
            quer_res = session.query(Rides).all()
            response_all = []
            for row in quer_res:
                response_all.append(row.ride_id)
            response = {"Result" : response_all}
        #RETURN RESPONSE
    return response

#DB CLEAR
def DBClear():
        #CLEAR BOTH MASTER'S DB AND SHARED DB
    session.query(Rides).delete()
    session.query(Users).delete()
    session1.query(Rides).delete()
    session1.query(Users).delete()
    session.commit()
    session1.commit()
    return {"Result":1}

#INITIALISE CONSUMER TAGS FOR BASIC CONSUME FOR RESPECTIVE QUEUES
read_tag = ''
write_tag = ''
sync_tag = ''

#CONNECT TO RABBITMQ SERVER
connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq-server', heartbeat=0))
#CREATE A CHANNEL
channel = connection.channel()

#DECLARE A DIRECT EXCHANGE FOR RPC, BOTH READ AND WRITE
channel.exchange_declare(exchange='rpc', exchange_type='direct')

#DECLARE READ QUEUE 
channel.queue_declare(queue='read_queue')
#BIND THE RPC DIRECT EXCHANGE TO READ QUEUE
channel.queue_bind(exchange='rpc', queue='read_queue', routing_key='read_queue')

#DECLARE WRITE QUEUE
channel.queue_declare(queue='write_queue')
#BIND THE RPC DIRECT EXCHANGE TO WRITE QUEUE 
channel.queue_bind(exchange='rpc', queue='write_queue', routing_key='write_queue')

#DECLARE FANOUT EXCHANGE FOR SYNC
channel.exchange_declare(exchange='sync', exchange_type='fanout')
#DECLARE SYNC QUEUE
channel.queue_declare(queue=worker_name)
#BIND THE SYNC QUEUE TO SYNC FANOUT EXCHANGE
channel.queue_bind(exchange='sync', queue=worker_name)

#CALLBACK FUNCTION 
def onSync_request(ch, method, props, body):
    #DESTRINGIFY
    req = ast.literal_eval(body.decode())
    #CHOOSE BASED ON REQUEST SENT
    if("clearDB" in req):
        response = DBClear()
    else:
        response = DBWrite(req)

#HANDLER FOR SYNC
def slave_synchronisation(req):
    #MASTER PUBLISHES THE REQUEST ON EXCHANGE
    channel.basic_publish(exchange='sync', routing_key='', body=str(req))

#CALLBACK FOR READ REQUEST
def onRead_request(ch, method, props, body):
    #DESTRINIFY JSON
    req = ast.literal_eval(body.decode())
    #GET READ RESPONSE
    response = DBRead(req)
    #PUBLISH  READ RESPONSE ON RESPONSE QUEUE
    #USE SAME CORRELATION ID FOR THE RESPONSE
    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         props.correlation_id),
                     body=str(response))
    #ACKNOWLEDGE
    ch.basic_ack(delivery_tag=method.delivery_tag)

#CALLBACK FOR WRITE REQUEST
def onWrite_request(ch, method, props, body):
    #DESTRINGIFY JSON
    req = ast.literal_eval(body.decode())
    #FORWARD BASED ON WRITE OR CLEAR
    if('clearDB' in req):
        response = DBClear()
    else:
        response = DBWrite(req)
        #PUBLISH RESPONSE TO RESPONSE QUEUE
        #USE SAME CORRELATION ID
    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         props.correlation_id),
                     body=str(response))
    #ACKNOWLEDGE
    ch.basic_ack(delivery_tag=method.delivery_tag)
        #ONCE WRITE IS DONE, MASTER PERFORMS SYNCHRONISATION
    slave_synchronisation(req)

#THIS FUNCTION ACTIVATES OR DEACTIVATES QUEUES BASED ON THE ROLE OF THE WORKER :  MASTER OR SLAVE
def set_consumer():
        global read_tag
        global write_tag
        #GET MASTER PID FROM ROOT NODE
        master_pid, stat = zk.get('/election')
        #IF THE CURRENT WORKER'S PID IS SAME AS THAT IN ROOT NODE THEN NO CHANGE
        #KEEP CONSUMING FROM WRITE
        if(int(worker_pid) == int(master_pid.decode("utf-8"))):
                #CONSUME FROM WRITE QUEUE
                write_tag = channel.basic_consume(queue='write_queue', on_message_callback=onWrite_request)
                if(read_tag is not None):
                        #IF READ QUEUE IS ACTIVE, DEACTIVATE
                        channel.basic_cancel(consumer_tag=read_tag)
                #if(sync_tag is not None):
                        #channel.basic_cancel(consumer_tag=sync_tag)
        else:
                #IF A SLAVE, CONSUME FROM READ QUEUE
                read_tag = channel.basic_consume(queue='read_queue', on_message_callback=onRead_request)
                if(write_tag is not None):
                        #IF IT WERE A MASTER, DEACTIVATE WRITE QUEUE
                        channel.basic_cancel(consumer_tag=write_tag)
                #IF A SLAVE, CONSUME FROM SYNC QUEUE
                sync_tag = channel.basic_consume(queue=worker_name, on_message_callback=onSync_request, auto_ack=True)


#LEADER ELECTION BASED ON PID
#SETTING A DATAWATCH ON ROOT NODE'S DATA
#IT IS TRIGGERED ONCE THERE IS ANY CHANGE IN THE DATA OF THE NODE
@zk.DataWatch('/election')
def watch_node(data, stat, event):
        if(data is not None):
                #DATA CONTAINS DATA OF ROOT NODE
                #STAT IS THE STATUS OF THE NODE
                #EVENT CONTAINS THE KAZOO EVENT THAT TRIGGERED IT

                #IF WORKER PID IS SAME AS MASTER'S THEN IT IS MASTER
                #NO NEED TO CHANGE
                if(int(worker_pid) == int(data.decode("utf-8"))):
                        logging.warning('recursion calls ignored')
                        return
                #ELSE WORKER ACQUIRES LOCK
                #CHECKS WHETHER IT CAN BECOME MASTER
                lock = zk.Lock('/lock', worker_name)
                with lock:
                        #IF PID OF WORKER IS LESS THAN MASTER'S THEN,
                        #SET CURRENT WORKER AS MASTER
                        if(int(worker_pid)< int(data.decode('utf-8'))):
                                logging.warning(worker_pid + ' is less than ' + data.decode('utf-8'))
                                logging.warning(worker_name + 'is master')
                                #SET ROOT NODE DATA AS PRESENT WORKER PID
                                zk.set('/election', bytes(worker_pid, 'utf-8'))
                        else:
                                #ELSE IT IS SLAVE
                                logging.warning(worker_pid + ' is not less than ' + data.decode('utf-8'))
                                logging.warning(worker_name + ' is slave ')
        else:
                logging.warning('master_check: data is empty')

        #ONCE THE ROLE OF THE WORKER IS SET
        #THE SET_CONSUMER FUNCTION IS CALLED WHICH ACTIVATES OR DEACTIVATES QUEUE BASED ON THAT ROLE
        set_consumer()


print(" [x] Awaiting RPC requests")
#START CONSUMING FROM QUEUES: IT LISTENS ENDLESSLY
channel.start_consuming()