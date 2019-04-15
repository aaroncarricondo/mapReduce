'''
Created on 13 mar. 2019

@author: aaroni34
'''

import sys, yaml, numpy, pika, time
from src import cos_backend, ibm_cf_connector
from time import sleep

#-----------------------------------------------------------------------------
#--------------------------Initializations-----------------------------------
#-----------------------------------------------------------------------------
#Load COS, Functions and RabbitAMQP information
with open('ibm_cloud_config', 'r') as config_file:
    try:
        
        res = yaml.safe_load(config_file)
        
    except yaml.YAMLError as exc:
        
        print(exc)

#Instantiate connector and cos
connector = ibm_cf_connector.CloudFunctions(res['ibm_cf'])
cos = cos_backend.cos_backend(res['ibm_cos'])

#-----------------------------------------
#----------------- COS -------------------
#Eliminate previous dictionaries done:
for i in cos.list_objects("noobucket", "finalDict"):
    cos.delete_object("noobucket", str(i))
    
for i in cos.list_objects("noobucket", "map"):
    cos.delete_object("noobucket", str(i))

#-----------------------------------------------
#----------------- Functions -------------------
#Create map function
compZip = open("map.zip", 'rb')
connector.create_action("map", compZip.read())
#Create reduce function
compZip = open("reduce.zip", 'rb')
connector.create_action("reduce", compZip.read())

#-----------------------------------------------
#---------------- RabbitAMQP -------------------
#Get URL
rabbit = res['ibm_rabbit']

# Declare connection and new queue on RABBITAMQ
params = pika.URLParameters(str(rabbit))
connection = pika.BlockingConnection(params)
channel = connection.channel() # start a channel

#----------------------
#----- MAP QUEUE ------
channel.queue_delete('map_queue')
channel.queue_declare(queue='map_queue')

# Start our counter at 0
messages = 0

# Method that will receive our messages and stop consuming
def callback_map(channel, method, header, body): 
    
    print ("File %r generated" % body)
    
    # Acknowledge message receipt (Eliminate receipt messages)
    channel.basic_ack(delivery_tag = method.delivery_tag)
    
    # We've received numDiv * 2 messages, stop consuming
    global messages 
    messages += 1
    if messages >= (numDiv * 2):
        channel.stop_consuming()

channel.basic_consume(callback_map, queue='map_queue')

#----------------------
#---- REDUCE QUEUE ----
channel.queue_delete('reduce_queue')
channel.queue_declare(queue='reduce_queue')

# Method that will receive our messages and stop
def callback_reduce(channel, method, header, body): 
    
    print ("File %r generated" % body) 
    
    # Acknowledge message receipt (Eliminate receipt messages)
    channel.basic_ack(delivery_tag = method.delivery_tag)
    
    # We've received numDiv * 2 messages, stop consuming
    global messages 
    messages += 1
    if messages >= 2:
        channel.stop_consuming()

#Not declaring the basic consume now.

#-----------------------------------------------------------------------------
#--------------------------START WORD COUNT-----------------------------------
#-----------------------------------------------------------------------------

start_time = time.time()
#Get parameters, # partitions, filename and finally file size
numDiv = int(sys.argv[1])
fileName = str(sys.argv[2])
size = int(cos.head_object("noobucket", fileName).get("content-length"))

#Create the interval array
#Partitions number and add 1 to size --> last chunk problem
chunk = int(size/numDiv)
if ((size % numDiv) != 0):
    chunk = chunk + 1 + int((size % numDiv) / numDiv)

#Fill the interval array
intervals = numpy.arange(0, size, chunk).tolist()
intervals.append(size)

#Print variable values
print("Size: " + str(size))
print("Chunk: " + str(chunk))
print("NumDivs: " + str(numDiv))
print ("Intervals: ", intervals)

print("Let's count:")

#Map fixed parameters
params = res['ibm_cos']
params.update({"fileName" : fileName, "ibm_rabbit" : rabbit})

#-------------------------------
#------MAP------COUNTING--------
#-------------------------------
for i in range(0, numDiv):
    
    #Define start byte and last byte for every map
    start = str(intervals[i])
    fi = str(intervals[i+1] - 1)
    
    #WordCount
    params.update({"start" : start, "fi" : fi, "resultName" : "mapWC" + str(i), "option" : "WC"})
    connector.invoke("map", params) 
    
    #CountWords
    params.update({"resultName" : "mapCW" + str(i), "option" : "CW"})
    connector.invoke("map", params)
       
    
#-------------------------------
#------MAP------CONSUME---------
#-------------------------------
print(' Waiting for mappers messages:')
channel.start_consuming()

#print("Done, now Reduce")
#--------------------------------
#------------REDUCE-------------- 
#--------------------------------
#Fill params
params = res['ibm_cos']
params.update({"ibm_rabbit" : rabbit})

#-------WORD COUNT------------    
params.update({"numDiv" : str(numDiv), "resultName" : "finalDictWC", "option" : "WC"})
connector.invoke("reduce", params)

#-------COUNT WORDS------------
params.update({"numDiv" : str(numDiv), "resultName" : "finalDictCW" , "option" : "CW"})
connector.invoke("reduce", params)


#-------------------------------
#----REDUCE------CONSUME--------
#-------------------------------
# Restart counter
messages = 0
#Define new queue consume with callback_reduce
channel.basic_consume(callback_reduce, queue='reduce_queue')
#Start consuming
print(' Waiting for reducers messages:')
channel.start_consuming()

#Take end time
end_time = time.time()

#Delete temporary map Files
for i in cos.list_objects("noobucket", "map"):
    cos.delete_object("noobucket", str(i))

#Check final dictionary
#WordCount
#print(cos.get_object("noobucket", "finalDictWC").decode('utf-8-sig'))
#CountWords
print(cos.get_object("noobucket", "finalDictCW").decode('utf-8-sig'))
print( "Time: ", end_time - start_time, "seconds")

