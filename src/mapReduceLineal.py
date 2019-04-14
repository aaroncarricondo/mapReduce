'''
Created on 20 mar. 2019

@author: aaroni34
'''

from collections import OrderedDict
import yaml, time
from src import cos_backend

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
cos = cos_backend.cos_backend(res['ibm_cos'])

#Start timing
start = time.time()
###########
#Read file
f = open("gutenberg-100M.txt", "rb")
inputString = f.read().decode('latin-1')


#Delete punctuation signs
# Define punctuation
punctuations = '''!()-[]{};:'"\,<>.?@#$%^&*_~=\n\r\t'''

#Substitute
for char in punctuations:
    inputString = inputString.replace(char,' ')

inputString = inputString.lower()

#Delete space key
inputString = filter(None, inputString.split(' '))

dicts = {}
dicts2 = {}

#Word Count
for word in inputString:
    if word in dicts:
        value = dicts.get(word)
        value+=1
    else:
        value = 1

    dicts.update({word : value})

#Two times for simulating orchestrator 
###########
#Read file
f = open("gutenberg-100M.txt", "rb")
inputString = f.read().decode('latin-1')

#Start timing
start = time.time()
#Delete punctuation signs
# Define punctuation
punctuations = '''!()-[]{};:'"\,<>.?@#$%^&*_~=\n\r\t'''

#Substitute
for char in punctuations:
    inputString = inputString.replace(char,' ')

inputString = inputString.lower()

#Delete space key
inputString = filter(None, inputString.split(' '))

#Counting Words
for word in inputString:
    
    if ("word") in dicts2:
        value2 = dicts2.get("word")
        value2 += 1
    else:
        value2 = 1
    
    dicts2.update({"word" : value2})

dicts = dict(OrderedDict(sorted(dicts.items(), key = lambda x: x[1])))

#end time
end = time.time()

#print(dicts)
print(dicts2)
print( "Time: ", end - start, "seconds")
    
