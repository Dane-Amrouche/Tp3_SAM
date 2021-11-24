import paho.mqtt.client as mqtt
import json
from logging import log
import time
from threading import Thread, Event
import os
import sys
import logging

class Connexion(Thread):
#les attributs de connexion
    connection = None
    mqtt_pub_topic=None
    mqtt_sub_topic=None
    unitId = None
#initialisation
    def __init__(self,unitId,mqtt_pub_topic,mqtt_sub_topic,mqtt_server,mqtt_port):
        super().__init__()
        self.unitId = unitId
        self.mqtt_pub_topic = mqtt_pub_topic
        self.mqqt_sub_topic = "1r1/O14/shutter/command"
        self.mqtt_server = "192.168.0.206"
        self.mqtt_port = 1883
        print("server = {} , port = {}".format(mqtt_server,mqtt_port))
        #configuration de la connexion mqtt
        self.connection = mqtt.Client()
        print("init called ahhhhh !!!")
        
        self.connection.on_connect = self.on_connect
        self.connection.on_publish = self.on_publish
        self.connection.on_subscribe = self.on_subscribe
        self.connection.on_message = self.on_message
        self.connection.on_disconnect = self.on_disconnect
        #self.connection.connect(self.mqtt_server,self.mqtt_port)
        self._shutdownEvent = Event()
     # called by Threading.start()
    def run( self ):


        # start connection
        print("start MQTT connection to '%s:%d' ..." % (self.mqtt_server,self.mqtt_port))
        self.connection.connect(self.mqtt_server,self.mqtt_port)
        #self.connection.subscribe(self.mqqt_sub_topic)
        # launch
        try:
            while not self._shutdownEvent.is_set():
                if self.connection.loop(timeout=2.0) != mqtt.MQTT_ERR_SUCCESS:
                    print("loop failed, sleeping a bit before retrying")
                    time.sleep(2)
        except Exception as e:
            print("shutdown activated ...")

        # shutdown module
        # disconnect ...
        self.connection.disconnect()

        # end of thread
        print("Thread end ...")



    #code pro http://www.steves-internet-guide.com/client-connections-python-mqtt/
    def on_connect(self,client, userdata, flags, rc):
        if rc==0:
            #self.connection.connected_flag=True #set flag
            print("connected OK Returned code=",rc)
            self.connection.subscribe(self.mqtt_sub_topic)   # QoS=0 default
            
        else:
            print("Bad connection Returned code= ",rc)
            #self.connection.bad_connection_flag=True


    ''' prepares and sends a payload in a MQTT message '''
    def send_message(self, topic, payload):

        if 'unitID' not in payload:
            payload['unitID'] = self.unitId

        if( payload['unitID'] is None ):
            print("tried to publish a message while not having a unitID ... aborting")
            return
        self.connection.publish(topic, json.dumps(payload))

    def on_disconnect(self,client,userdata,rc):
        print("raison de la deconnection : ",rc)
        #self.connection.connected_flag=False
        self.connection_disconnected_flag=True

    def on_message(self,client,userdata,msg):
        print("on_message called")
        print("message reçu " + str(msg.payload) + "du topic "+msg.topic)
        payload = json.loads(msg.payload)
        print("payload = ",payload)
        if(payload['unitID']=="all"):
            print("all is received")
            self.handle_Msg(payload)

        
       
        self.curCmd = payload['order']
        data = {
            "unitId" : self.unitID,
            "status" : self.status,
            "order" : payload['order']
        }
        print(data)
        data_out = json.dumps(data)
        self.connection.publish(self.mqtt_pub_topic,data_out)
        

    def handle_Msg(self,payload):
        pass

    def on_publish(self,client,userdata,mid):
        pass

    def on_subscribe(self,client,userdata,mid,granted_qos):
        print("subscribed")