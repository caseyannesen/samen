##mqtt based message handler for intercommunication between the modules
##should cache requests and resend them upto 5 times if no response is received
##should be able to handle multiple requests at the same time
## use as much async as possible

import paho.mqtt.client as mqtt
import json, time, argparse, hashlib


MESSAGE_TEMPLATE = {
    "id": "",   #unique id for the message set to the hash of the json string of data encoded as utf-8 ""
    "type": "", #type of the message [notice, request, response, error]
    "from": "", #id of the sending module
    "to": "all",   #id of the receiving module
    "timestamp": "", #timestamp of the message
    "data": { #data to be sent
        'message': "", # message to be sent
        'type': "", #type of data [text, image, video, audio, file, json]
    },      
    "cache": False, #whether to cache the message or not
    }

BROKER_DATA = {
    'username':"onverantwoordelik", 'password':"asdf8090ABC!!", 'subscribe_to': ['testout'], 'publish_to': ['testin'], 
    'address':'3a4f7d6b0cd1473681d6c9bdfa569318.s2.eu.hivemq.cloud',
    'mqtt_port':8883, 'ws_port':8884, 'use_websockets':False,
    'timeout':3600, "client_id": "testout"
}


class MqttMessageHandler:
    # initializes the client
    def __init__(self, broker_data=BROKER_DATA, call_backs={}, my_id="") -> None:
        if broker_data:
            self.broker_data = broker_data
            self.client = self.get_client(broker_data=broker_data)
        else:
            self.client = None

        if call_backs:
            self.on_connect = call_backs.get('on_connect', self.on_connect)
            self.on_message = call_backs.get('on_message', self.on_message)
            self.on_disconnect = call_backs.get('on_disconnect', self.on_disconnect)

    #returns a client instance
    def get_client(self, broker_data=None, clean_session=True):
        resp = False
        if broker_data: # Create a client instance
            client = mqtt.Client(client_id=broker_data['client_id'])  # Replace with your desired client_id
            client.username_pw_set(broker_data['username'], broker_data['password'])# Set credentials
            client.on_connect = self.on_connect # Assign the callbacks to the client
            client.on_message = self.on_message # Assign the callbacks to the client
            client.on_disconnect = self.on_disconnect # Assign the callbacks to the client
            client.reconnect_delay_set(min_delay=1, max_delay=60) #automatically reconnect after 1 second and increase the delay to 60 seconds
            while True:
                try:
                    if broker_data['use_websockets']:# Configure MQTT broker using WebSockets
                        client.ws_set_options(path="/mqtt")
                        port = broker_data['ws_port']
                    else:# Configure MQTT broker using SSL/TLS
                        client.tls_set()
                        port = broker_data['mqtt_port']
                    client.connect(broker_data['address'], port, keepalive=broker_data['timeout']) # Connect to MQTT broker
                    break
                except KeyboardInterrupt:# Disconnect and stop the network loop when manually interrupted
                    client.disconnect()
            resp = client
        return resp
    
    #notifies connetion to subscribers.
    def on_connect(self, client, userdata, flags, rc):
        if self.broker_data and rc == 0:
            print('connected')
            self.subscribe_to_topics(self.broker_data['subscribe_to'])
            msg = {"type": "notice", "data": {"message": "connected", 'type':'text'},"cache": False}
            message = self.prepare_message(msg)
            self.publish_to_topics(self.broker_data['publish_to'], message)

    def on_disconnect(self, client, userdata, rc):
        pass

    # handles all the messages received
    def on_message(self, client, userdata, msg):
        print(msg.topic+" "+str(msg.payload))

    #subscribes to all the topics
    def subscribe_to_topics(self, topics):
        for topic in topics:
            self.client.subscribe(topic)

    #publishes to all the topics
    def publish_to_topics(self, topics, message):
        for topic in topics:
            self.client.publish(topic, payload=json.dumps(message), qos=2, retain=True)

    #starts the client
    def start(self):
        if self.client:
            print(F"starting client {self.client}")
            self.client.loop_start()

    #stops the client
    def stop(self):
        if self.client:
            self.client.loop_stop()

    #prepares the message to be sent
    def prepare_message(self, message_data, message_type, to='all', cache=False):
        message = MESSAGE_TEMPLATE.copy()
        message['data']['message'] = message_data
        message['data']['type'] = message_type
        message['id'] = hashlib.md5(json.dumps(message['data']).encode('utf-8')).hexdigest()
        message.update({'from': self.broker_data['client_id'], 'to':to, 'timestamp': time.time(), 'cache': cache})
        return json.dumps(message)
    
    def send_message(self, message="", type="notice", to="all", cache=None):
        message = self.prepare_message({'type': type, 'to': to, 'data': {'message': message}}, cache=cache)
        self.publish_to_topics(self.broker_data['publish_to'], message)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run CHEAPRAY network manager')
    parser.add_argument('--client', type=str, default='nitb', help='Client ID to run as')
    parser.add_argument('--remote', type=str, default='nitb', help='Client ID to run as')
    args = parser.parse_args()
    broker_data = BROKER_DATA.copy()
    if args.client and args.remote:
        broker_data.update({'client_id': args.client, 'subscribe_to': [args.client], 'publish_to': [args.remote]})

    message_handler = MqttMessageHandler(broker_data=broker_data, my_id=args.client)
    print(F'test client for mqtt message handler params = {message_handler}')
    message_handler.start()
    while True:
        message_handler.send_message(message=input("enter message: "), type="notice", to="all")
