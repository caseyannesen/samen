##mqtt based message handler for intercommunication between the modules
##should cache requests and resend them upto 5 times if no response is received
##should be able to handle multiple requests at the same time
## use as much async as possible

import paho.mqtt.client as mqtt
import json, time, argparse, hashlib
from cache import cache as ch

MESSAGE_TEMPLATE = {
    "id": "",   #unique id for the message set to the hash of the json string of data encoded as utf-8 ""
    "type": "", #type of the message [notice, request, response, error]
    "from": "", #id of the sending module
    "to": "all",   #id of the receiving module
    "timestamp": "", #timestamp of the message
    "data": { #data to be sent
        'message': "", # message to be sent
        'type': "", #type of data [text, image, video, audio, file, json]
    }, 'req_id': "", #id of the request if the message is a response
    're_timestamp': "", #timestamp of the request if the message is a response
    "cache":[False, 0], #whether to cache the message or not
    }

BROKER_DATA = {
    'username':"onverantwoordelik", 'password':"asdf8090ABC!!", 'subscribe_to': ['testout'], 'publish_to': ['testin'], 
    'address':'3a4f7d6b0cd1473681d6c9bdfa569318.s2.eu.hivemq.cloud',
    'mqtt_port':8883, 'ws_port':8884, 'use_websockets':False,
    'timeout':3600, "client_id": "testout"
}


class MqttMessageHandler:
    # initializes the client
    def __init__(self, broker_data=BROKER_DATA, call_backs={}, handlers={'request':None,'message':None,'response':None} my_id="") -> None:
        if broker_data:
            self.broker_data = broker_data
            self.client = self.get_client(broker_data=broker_data)
        else:
            self.client = None

        if call_backs:
            self.on_connect = call_backs.get('on_connect', self.on_connect)
            self.on_message = call_backs.get('on_message', self.on_message)
            self.on_disconnect = call_backs.get('on_disconnect', self.on_disconnect)
        
        self.request_handler = handlers.get('request', self.printboy)
        self.message_handler = handlers.get('message', self.printboy)
        self.response_handler = handlers.get('response', self.printboy)

    def printboy(self, message):
        print(F"Got message: {message}")

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
            self.subscribe_to_topics(self.broker_data['subscribe_to'])
            message = self.prepare_message(message_data={'message':'connected', 'data_type':'text', 'type':'notice'}, cache=[False, 0])
            self.publish_to_topics(self.broker_data['publish_to'], message)

    def on_disconnect(self, client, userdata, rc):
        pass
    
    # function to cache a response
    def cache_message(self, message, data=None):
        can_cache, timeout = message['cache']  # check if we can cache the message (responses can be cached)
        if can_cache and timeout: #check if cache is enabled
            if message['type'] == 'response': # if cache response
                ch.set(message['req_id'], {'value': message, 'timestamp': time.time(), 'timeout': timeout})
            data = ch.get(message['id']) # get the cached message
        if data: # timeout checker
            if time.time() - data['timestamp'] > data['timeout']:
                ch.delete(message['id'])
                data = None
            else:
                data = data['value']
        return (data, message)

    
    # handles all the messages received
    def on_message(self, client, userdata, msg):
        response, message = self.cache_message(json.loads(msg.payload))
        if response:
            pass
        self.handle_message(message, skip=False)
    
    #subscribes to all the topics
    def subscribe_to_topics(self, topics):
        for topic in topics:
            self.client.subscribe(topic)

    #publishes to all the topics
    def publish_to_topics(self, topics, message):
        for topic in topics:
            self.client.publish(topic, payload=json.dumps(message), qos=2, retain=False)

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
    def prepare_message(self, message_data={}, respond_to={}, cache=[False, 0]):
        message = MESSAGE_TEMPLATE
        if not respond_to:
            message['data'] = {'message': message_data['message'], 'type': message_data['data_type']}
            message['to'], message['from'], message['type'] = message_data.get('to', 'all'), self.broker_data['client_id'], message_data['type']
            message['id'] = hashlib.md5(json.dumps(message['data']).encode('utf-8')).hexdigest()
            message['timestamp'], message['cache'] = time.time(), cache
        elif respond_to:
            message['data'] = {'message': message_data['message'], 'type': message_data['type']}
            message['to'], message['from'] = respond_to['from'], self.broker_data['client_id']
            message['id'] = hashlib.md5(json.dumps(message['data']).encode('utf-8')).hexdigest()
            message['req_id'], message['timestamp'] = respond_to['id'], respond_to['timestamp']
            message['re_timestamp'], message['cache'], message['type'] = time.time(), respond_to['cache'], 'response'
        return message
    
    def send_to_action(self, message):
        response, message = self.cache_message(message)
        return [True, response] if response else [False, message]
            
    def send_message(self, message_data={}, reply_to={}, cache=[False, 0]):
        message = self.prepare_message(message_data=message_data, respond_to=reply_to, cache=cache)
        skip, response = self.send_to_action(message)
        if skip:
            self.handle_message(response, skip=True)
        else:
            self.publish_to_topics(self.broker_data['publish_to'], message)
        return True

    def handle_message(self, message, skip=False):
        if message['type'] == 'request' and self.request_handler:
            self.request_handler(message)
        elif message['type'] == 'response' and self.response_handler:
            self.response_handler(message)
        elif self.message_handler:
            self.message_handler(message)


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
        inp = input("enter message: ")
        inp = inp.split(' ')
        if len(inp) > 6:
            m_type, data_type, to, cache, timeout, message = inp[0], inp[1], inp[2], inp[3], inp[4], ' '.join(inp[5:])
        else:
            m_type, data_type, to, cache, timeout, message = 'notice', 'text', 'all', True, 100, ' '.join(inp)
        message_data = {'message': message, 'data_type': data_type, 'type': m_type, 'to': to}
        message_handler.send_message(message_data=message_data, cache=[cache, int(timeout)])
