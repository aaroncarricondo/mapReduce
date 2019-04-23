'''
Created on 20 mar. 2019

@author: aaroni34
'''

from collections import OrderedDict
import yaml, time, sys
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

#-----------------------------------------------------------------------------
#--------------------------     OPTION     -----------------------------------
#-----------------------------------------------------------------------------
option = 2

while (option != 0 and option != 1):
    print("0. WordCount")
    print("1. Counting Words")
    option = int(input("Choose:"))

#Instantiate connector and cos
cos = cos_backend.cos_backend(res['ibm_cos'])

#Start timing
start = time.time()
###########
#Read file
file_name = sys.argv[1]
f = open(file_name, "rb")
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
    
if (option == 0):   
    #Word Count
    for word in inputString:
        if word in dicts:
            value = dicts.get(word)
            value+=1
        else:
            value = 1
    
        dicts.update({word : value})
        
    dicts = dict(OrderedDict(sorted(dicts.items(), key = lambda x: x[1])))

else:

    dicts2 = {}
    #Counting Words
    for word in inputString:
        
        if ("word") in dicts2:
            value2 = dicts2.get("word")
            value2 += 1
        else:
            value2 = 1
        
        dicts2.update({"word" : value2})
    

#end time
end = time.time()

#print(dicts)
#print(dicts2)
print( "Time: ", end - start, "seconds")
    
