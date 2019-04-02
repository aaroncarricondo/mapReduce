'''
Created on 13 mar. 2019

@author: aaroni34
'''

from src import cos_backend
from src import ibm_cf_connector
import sys
import yaml
import numpy
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

#Create map functions
compZip = open("mapCW.zip", 'rb')
connector.create_action("mapCW", compZip.read())

compZip = open("mapWC.zip", 'rb')
connector.create_action("mapWC", compZip.read())

#Get parameters, # partitions and filename
numDiv = int(sys.argv[1])
fileName = str(sys.argv[2])

size = int(cos.head_object("noobucket", fileName).get("content-length"))

#Numero de divisions i sumem 1 a size, per a que l'ultim chunk s'afagi be
chunk = int(size/numDiv)
size = size


#Definim i omplim la taula de intervals
intervals = numpy.arange(0, size, chunk).tolist()

#If chunk is not multiple, one more Div
if((size % chunk) != 0):
    numDiv = numDiv + 1
    
intervals.append(size)

#Print variable values
print("Size: " + str(size))
print("Chunk: " + str(chunk))
print("NumDivs: " + str(numDiv))
print (intervals)

#Map fixed parameters
params = res['ibm_cos']
params.update({"fileName" : fileName})


#-------------------------------
#------MAP------COUNTWORDS------
#-------------------------------
for i in range(0, numDiv):
    
    start = str(intervals[i])
    fi = str(intervals[i+1] - 1)
    #Add new values for the keys in dictionary
    params.update({"start" : start, "fi" : fi, "resultName" : "mapCW"+str(i)})
    
    connector.invoke("mapCW", params)

#-----------------------------
#Prove that map it's done:
list = cos.list_objects("noobucket", "mapCW")
while (len(list) != numDiv ):
    sleep(1.5)
    list = cos.list_objects("noobucket", "mapCW")
    print (list, len(list))
    

#-------------------------------
#------MAP------WORDCOUNT------
#-------------------------------    

for i in range(0, numDiv):
    
    start = str(intervals[i])
    fi = str(intervals[i+1] - 1)
    #Add new values for the keys in dictionary
    params.update({"start" : start, "fi" : fi, "resultName" : "mapWC"+str(i)})
    
    
    connector.invoke("mapWC", params)    

#-----------------------------
#Prove that map it's done:
list = cos.list_objects("noobucket", "mapWC")
while (len(list) != numDiv ):
    sleep(1.5)
    list = cos.list_objects("noobucket", "mapWC")
    print (list, len(list))  


print("Done, now Reduce")

#--------------------------------
#------------Reduce-------------- 
#--------------------------------

compZip = open("reduce.zip", 'rb')
connector.create_action("reduce", compZip.read())

#Fill params
params = res['ibm_cos']
params.update({"numDiv" : str(numDiv), "resultName" : "finalDict"})

#--------------REDUCE----------------
connector.invoke("reduce", params)

while(len(cos.list_objects("noobucket", "final")) != 1):
    sleep(1)


#Check final dictionary
print(cos.get_object("noobucket", "finalDict").decode('utf-8-sig'))

