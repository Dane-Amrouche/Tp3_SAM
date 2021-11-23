#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Shutter module
#
# Thiebolt  aug.19  updated
# Francois  apr.16  initial release
#



# #############################################################################
#
# Import zone
#
import time
import json
import threading
import paho.mqtt.client as mqtt_client
import os
import sys
import logging
from connection import Connexion


# #############################################################################
#
# Functions
#



# #############################################################################
#
# Classes
#
class Shutter(Connexion):

    # class attributes
    SHUTTER_POS_CLOSED  = 0
    SHUTTER_POS_OPEN    = 1
    SHUTTER_POS_UNKNOWN = 2

    SHUTTER_ACTION_CLOSE    = 0
    SHUTTER_ACTION_OPEN     = 1
    SHUTTER_ACTION_STOP     = 2
    SHUTTER_ACTION_IDLE     = 3
    SHUTTER_ACTION_UNKNOWN  = 4

    SHUTTER_TYPE_WIRED = 0
    SHUTTER_TYPE_WIRELESS = 1

    MQTT_TYPE_TOPIC = "shutter"

    # Min. and max. values for shutter course time
    MIN_COURSE_TIME         = 5
    MAX_COURSE_TIME         = 60

    # attributes
    _status = SHUTTER_POS_UNKNOWN
    shutterType = SHUTTER_TYPE_WIRED
    courseTime  = 30;       # (seconds) max. time for shutter to get fully open / close

    _backend    = None      # current backends
    _upOutput   = None
    _downOutput = None
    _stopOutput = None

    _curCmd     = None
    _condition  = None      # threading condition
    _thread     = None      # thread to handle shutter's course

    #Attributs MQTT Config
    mqtt_server = "192.168.0.206"
    mqtt_port = 1883
    mqtt_pub_topic = "1r1/014/shutter"
    mqtt_sub_topic = "1r1/014/shutter/command"

    def __init__(self, unitID,mqtt_pub_topic="1r1/014/shutter",mqtt_sub_topic="1r1/014/shutter/command",mqtt_server="192.168.0.206",mqtt_port=1883,shutterType="wired", courseTime=30, io_backend=None, upOutput=None, downOutput=None, stopOutput=None, shutdown_event=None, *args, **kwargs):
        ''' Initialize object '''
    
        super().__init__(unitID,mqtt_pub_topic,mqtt_sub_topic,mqtt_server,mqtt_port)
        self.unitID = unitID


 

    def handle_Msg(self,payload):
        print("message received")
        if self.payload['order']=="Up":
            self.open()
        elif self.payload['order'] == "Down":
            self.close()
        elif self.payload['order'] == "Stop" :
            self.stop()
        self.curCmd = payload['order']
        data = {
            "unitId" : self.unitID,
            "status" : self.status,
            "order" : payload['order']
            }
        print(data)
        data_out = json.dumps(data)
        self.connection.publish(self.mqtt_pub_topic,data_out)             



    
    def status(self):
        print ("Le volet {} est {} ".format(self.unitID,self._status)) 

    def open(self):
        print ("J'ouvre le volet \n")
        self._status = Shutter.SHUTTER_POS_OPEN
        print ("Le volet {} est {} ".format(self.unitID,self._status))        
    

    def close(self):
        print ("Je ferme le volet \n")
        self._status = Shutter.SHUTTER_POS_CLOSED
        print ("Le volet {} est {} ".format(self.unitID,self._status)) 

    def stop(self):
        print ("STOP  !! \n")
        self._status = Shutter.SHUTTER_POS_UNKNOWN
        print ("Le volet {} est {} ".format(self.unitID,self._status))  
                 







# #############################################################################
#
# MAIN
#

def main():
    c1 = Shutter(1)
    c1.start()
    #c1.handle_Msg("Up")
    c1.status()
        
    
"""     
    #TODO: implement simple tests of your module
    s1 = Shutter(1)
    s2 = Shutter(2)

    s1.status()
    s2.status()

    s1.handle_Msg("Up")
    s2.handle_Msg("Down")

    s1.status()
    s2.status()

 """






# Execution or import
if __name__ == "__main__":

    # Logging setup
    logging.basicConfig(format="[%(asctime)s][%(module)s:%(funcName)s:%(lineno)d][%(levelname)s] %(message)s", stream=sys.stdout)
    log = logging.getLogger()

    print("\n[DBG] DEBUG mode activated ... ")
    log.setLevel(logging.DEBUG)
    #log.setLevel(logging.INFO)

    # Start executing
    main()


# The END - Jim Morrison 1943 - 1971
#sys.exit(0)

