import pika, time, flask, sqlite3, multiprocessing # , threading
from flask_cors import CORS
from requests import request

# ---------------------------------------------------------------------------------------------------------------------- #

class Dana:

    def __init__(self, d=[None, None, None, None, None, None, None, None, None, None]):

        self.computer = d[0]
        self.domain = d[1]
        self.ip = d[2]
        self.mac = d[3]
        self.cpu = d[4]
        self.ram = d[5]
        self.freq = d[6]
        self.gpu = d[7]
        self.freq_val = d[8]
        self.created = d[9]

    def to_dict(self):
        return {'created': self.created, 'computer': self.computer, 'domain': self.domain, 'ip': self.ip, 'mac': self.mac, 'cpu': self.cpu, 'ram': self.ram, 'freq': self.freq, 'gpu': self.gpu, 'freq_val': self.freq_val}

    def from_string(self, s):
        self.computer, self.domain, self.ip, self.mac, self.cpu, self.ram, self.freq, self.gpu, self.freq_val, self.created = s.strip().split(';')

    def from_host(self, computer, domain, ip, mac):
        self.computer = computer
        self.domain = domain
        self.ip = ip
        self.mac = mac

# ---------------------------------------------------------------------------------------------------------------------- #

def start_mqtt():

    def on_message(channel, method_frame, header_frame, body):

        print('### MQTT ###', method_frame.delivery_tag, body)
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        computer, domain, ip, mac, cpu, ram, freq, gpu, freq_val, created = body.decode().split(';') # computer, domain, ip, mac, cpu, ram, freq, gpu, freq_val, timestamp = body.decode().split(';')
        mac = 'mac'.upper()
        connection = sqlite3.connect('projekt.sqlite')
        hid = connection.cursor().execute('INSERT INTO hosts (computer, domain, ip, mac) VALUES (:computer, :domain, :ip, :mac)', {'computer': computer, 'domain': domain, 'ip': ip, 'mac': mac})
        connection.cursor().execute('INSERT INTO dane (created, computer, domain, ip, mac, cpu, ram, freq, gpu, freq_val) VALUES (:created, :computer, :domain, :ip, :mac, :cpu, :ram, :freq, :gpu, :freq_val)', {'computer': computer, 'domain': domain, 'ip': ip, 'mac': mac, 'cpu': cpu, 'ram': ram, 'freq': freq, 'gpu': gpu, 'freq_val': freq_val, 'created': created})
        connection.commit()

    params = pika.URLParameters('amqps://cpssdxpi:AURv9PPOThs9ue1xlX7CUJbGS-FWxDIb@sparrow.rmq.cloudamqp.com/cpssdxpi')
    params.socket_timeout = 5
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.basic_consume('sensors', on_message)
    print('starting consumer ...')
    channel.start_consuming()
    print('stopping consumer ...')
    channel.stop_consuming()
    connection.close()

# ---------------------------------------------------------------------------------------------------------------------- #

app = flask.Flask(__name__)
CORS(app, resources={r'/*': {'origins': '*'}})

@app.route('/test', methods=['GET',])
def test():
    print(flask.request.headers)
    return ''

@app.route('/get/computer/data', methods=['POST',])
def get_data():
    print(flask.request.headers)
    success = False
    dane = []
    
    created_from = int(flask.request.json['dataFrom'])
    created_to = int(flask.request.json['dataTo'])
    mac = str(flask.request.json['macAddress']).upper()

    print('from:', created_from, type(created_from), 'to:', created_to, type(created_to), 'mac:', mac)

    connection = sqlite3.connect('projekt.sqlite')
    cursor = connection.cursor()
    _dane = cursor.execute(f"SELECT computer, domain, ip, mac, cpu, ram, freq, gpu, freq_val, created FROM dane WHERE mac = '{mac}' AND created BETWEEN {created_from} AND {created_to}", {'created_from': created_from, 'created_to': created_to, 'mac' :mac}).fetchall()
    for d in _dane:
        dane.append(Dana(d).to_dict())
    success = True
    response = {'success': success, 'errors': [], 'content': {'length': len(dane), 'items': dane}}
    print(len(dane))
    return flask.jsonify(response)

@app.route('/get/computers', methods=['GET',])
def get_computers():

    success = False
    dane = []
    
    connection = sqlite3.connect('projekt.sqlite')
    cursor = connection.cursor()
    _dane = cursor.execute(f"SELECT computer, domain, ip, mac FROM hosts").fetchall()
    for d in _dane:
        dana = Dana()
        dana.from_host(d[0], d[1], d[2], d[3])
        dane.append(dana.to_dict())
    success = True
    response = {'success': success, 'errors': [], 'content': {'length': len(dane), 'items': dane}}
    return flask.jsonify(response)

# ---------------------------------------------------------------------------------------------------------------------- #

pid = multiprocessing.Process(target=start_mqtt, name='mqtt')
print('mqtt process pid:', pid.pid)

if __name__ == '__main__':
    pid.start()
    app.run(host='0.0.0.0', port='8080', debug=False)
