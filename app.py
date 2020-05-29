#/usr/bin/python 3
import paho.mqtt.client as mqtt
import psycopg2
import re
import signal



scope = {}

slovar = dict(topic  = '', mess = '' ,block = '', flag = '0',id_script = '',maxid_in_sequence = '',code = '')

CACHE = []



def execution_analytics_script():
	global conn
	global slovar
	global cursor
	global scope
	find_analytics()
	for element in slovar['id_script']:
		cursor.execute('SELECT MAX(id_in_sequnces) FROM chains where id = %s',(element,))
		slovar['maxid_in_sequence'] = re.findall(r'\d+',str(cursor.fetchall()))
		for block in range(1,int(slovar['maxid_in_sequence'][0])+1):
			slovar['block']= block
			cursor.execute("SELECT blocks.src FROM blocks WHERE blocks.id=(SELECT chains.is_block FROM chains WHERE (chains.id_in_sequnces = '%s' AND chains.id =%s))" %(block,element))
			slovar['code'] = str(cursor.fetchall())	
			stroka = slovar['code'][2:(len(slovar['code'])-3)]
			print(stroka)
			exec (stroka,scope)
			func(d)
			print(slovar)
			if slovar['flag'] == '1' :
				return 1
				

	return 0
			
		
		


def find_analytics():
	global conn
	global slovar
	global cursor
	cursor.execute('SELECT topics.id_analystics_script FROM topics where topics.topic = %s', (slovar['topic'],)  )
	slovar['id_script'] = re.findall(r'\d+',str(cursor.fetchall()))
	return 0
	

def on_connect(client, userdata, flags, rc):
	print('Connected with result code {}'.format(rc))
    
	client.subscribe([('#', 0)])

def on_message(client, userdata, message):
	topic = message.topic
	payload = message.payload.decode('utf8')
    	
	print('{}:{}'.format(topic,payload))
	global CACHE
    	
	CACHE.append((topic, payload))	
	
	
client = mqtt.Client();
client.on_connect = on_connect
client.on_message = on_message

client.username_pw_set('admin2','2277995500Qw')
client.connect('localhost',1883,60)


conn = psycopg2.connect(host='localhost',port = '5432', dbname='sammy', user='iot', password='2277995500Qw')
cursor = conn.cursor()
print('Connected to database') 

isItRunning = True

def sigterm_handler(signal, frame):
	print('SIGTERM caught')
    
	global isItRunning
	isItRunning = False
    
signal.signal(signal.SIGTERM, sigterm_handler)


try:
	while isItRunning:
		if len(CACHE)>0:
			for row in CACHE:
				topic, payload=row
				slovar['topic']=topic
				slovar['mess']=payload
				print(slovar["topic"])
				execution_analytics_script()
			conn.commit()
			CACHE = []
		
		client.loop(0.00001)

except KeyboardInterrupt:
	print('KeyboardInterrupt caught')




cursor.close()
conn.close()

