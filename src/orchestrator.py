'''
Created on 13 mar. 2019

@author: aaroni34
'''

from src import cos_backend
from src import ibm_cf_connector
import sys, yaml, numpy
import pika
from time import sleep


#Load COS and Fucntions information
with open('ibm_cloud_config', 'r') as config_file:
    try:
        
        res = yaml.safe_load(config_file)
        
    except yaml.YAMLError as exc:
        print(exc)


#Instantiate connector and cos
connector = ibm_cf_connector.CloudFunctions(res['ibm_cf'])
cos = cos_backend.cos_backend(res['ibm_cos'])


#Eliminate previous dictionaries done:
for i in cos.list_objects("noobucket", "finalDict"):
    cos.delete_object("noobucket", str(i))
    
for i in cos.list_objects("noobucket", "map"):
    cos.delete_object("noobucket", str(i))


#Create map functions
compZip = open("map.zip", 'rb')
connector.create_action("map", compZip.read())

#Get parameters, # partitions and filename
numDiv = int(sys.argv[1])
fileName = str(sys.argv[2])

size = int(cos.head_object("noobucket", fileName).get("content-length"))

#Numero de divisions i sumem 1 a size, per a que l'ultim chunk s'afagi be
chunk = int(size/numDiv)
if ((size % numDiv) != 0):
    chunk = chunk + 1 + int((size % numDiv) / numDiv)


#Definim i omplim la taula de intervals
intervals = numpy.arange(0, size, chunk).tolist()
intervals.append(size)

#Print variable values
print("Size: " + str(size))
print("Chunk: " + str(chunk))
print("NumDivs: " + str(numDiv))
print ("Intervals: ", intervals)

rabbit = res['ibm_rabbit']
#-----------------------------------------------------------------------------
# Declare connection and new queue on RABBITAMQ
# url = os.environ.get('CLOUDAMQP_URL', rabbit)
params = pika.URLParameters(str(rabbit))
connection = pika.BlockingConnection(params)
channel = connection.channel() # start a channel

channel.queue_declare(queue='map_queue', durable=True, exclusive=False, auto_delete=False)
#channel.queue_purge()

# Start our counter at 0
messages = 0

# Method that will receive our messages and stop consuming after 10
def callback_map(channel, method, header, body): 
    print ("Message:")
    print ("File %r generated" % body)
    
    # Acknowledge message receipt (Eliminate receipt messages)
    channel.basic_ack(delivery_tag = method.delivery_tag)
    
    # We've received numDiv messages, stop consuming
    global messages 
    messages += 1
    if messages >= (numDiv * 2):
        channel.stop_consuming()

channel.basic_consume(callback_map, queue='map_queue')

#-----------------------------------------------------------------------------
#Map fixed parameters
params = res['ibm_cos']
params.update({"fileName" : fileName, "ibm_rabbit" : rabbit})

print("Let's count:")
#-------------------------------
#------MAP------COUNTWORDS------
#-------------------------------
for i in range(0, numDiv):
    
    start = str(intervals[i])
    fi = str(intervals[i+1] - 1)
    #Add new values for the keys in dictionary
    #CountWords
    params.update({"start" : start, "fi" : fi, "resultName" : "mapCW" + str(i), "option" : "CW"})
    connector.invoke("map", params)
    
    #WordCount
    params.update({"resultName" : "mapWC" + str(i), "option" : "WC"})
    connector.invoke("map", params)    
    
#-----------------------------------------------------------------------------
#Try RABBITMQ QUEUE
    
print(' [*] Waiting for messages:')
channel.start_consuming()
#-----------------------------------------------------------------------------
print("Done, now Reduce")

#--------------------------------
#------------Reduce-------------- 
#--------------------------------

compZip = open("reduce.zip", 'rb')
connector.create_action("reduce", compZip.read())

#--------------REDUCE----------------
#Fill paramsx

params = res['ibm_cos']

#-------COUNT WORDS------------
params.update({"numDiv" : str(numDiv), "resultName" : "finalDictCW" , "option" : "CW"})
connector.invoke("reduce", params)

#-------WORD COUNT------------    
params.update({"numDiv" : str(numDiv), "resultName" : "finalDictWC", "option" : "WC"})
connector.invoke("reduce", params)

while(len(cos.list_objects("noobucket", "finalDict")) < 2):
    sleep(1)


#Check final dictionary
print(cos.get_object("noobucket", "finalDictCW").decode('utf-8-sig'))
print(cos.get_object("noobucket", "finalDictWC").decode('utf-8-sig'))

