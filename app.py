import paho.mqtt.client as mqtt
import psycopg2

conn = psycopg2.connect(dbname='sammy', user='sammy', 
			password='2277995500Qw', host='localhost')
						
HOST = 'localhost'
PORT = 5432


slovar = dict(topic  = '', mess = '' ,block = '', flag = '0')
list1 = list()


def execution_analytics_script():
	global conn
	global list1
	global slovar
	cursor = conn.cursor()
	for i in range(len(list1))
		kol_vo = cursor.execute('SELECT MAX(id_in_sequnces) FROM chains where id = %s', % list1[i])
		block=1
		for block in range(int(kol_vo))
			code = cursor.execute('SELECT blocks.src FROM blocks WHERE blocks.id=(SELECT chains.id_block FROM chains WHERE (chains.id_in_sequnces = %s AND chains.id =%s))', (block,list1[i]))
			exec code in slovar
	cursor.close()
	return slovar['flag']
			
		
		


def find_analytics():
	global conn
	global slovar
	global list
	cursor = conn.cursor()
	cursor.execute('SELECT id_analytics_script FROM topics where topic = %(topic)s', % slovar )
	for row in cursor:
		list = list + cursor
	cursor.close()
	return 0
	
	
	

def on_connect(client, userdata, flags, rc):
    print('Connected with result code {}'.format(rc))
    client.subscribe('#', 0)
    
def on_message(client, userdata, message):
    global slovar
    slovar['topic']= message.topic
	slovar['mess']= message.payload.decode('utf8')


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
print(execution_analytics_script())
print(find_analytics())






client.connect(HOST, PORT, 60)


client.loop_forever()
