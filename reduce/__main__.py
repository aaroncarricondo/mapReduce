'''
Created on 31 mar. 2019

@author: aaroni34
'''
import cos_backend
import ast, os, pika
from collections import OrderedDict

def main(args):
    #Get parameters
    numDiv = args["numDiv"]
    resultName = args["resultName"]
    option = args["option"]
    rabbit = str(args['ibm_rabbit'])
    
    #--------------------------------------------------------------------------------------
    # Access the CLODUAMQP_URL environment variable and parse it (fallback to localhost)
    url = os.environ.get('CLOUDAMQP_URL', rabbit)
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel() # start a channel
    channel.queue_declare(queue='reduce_queue', durable=True, exclusive=False, auto_delete=False) # Declare a queue
        
    #---------------------------------------------------------------------------------------
    
    #Instantiate COS
    #COS parameters
    endpoint = args["endpoint"]
    secret_key = args["secret_key"]
    access_key = args["access_key"]
    
    #Instanciate cos
    cos = cos_backend.cos_backend({ "endpoint" : endpoint, "secret_key" : secret_key, "access_key" : access_key})
    
    final_dict = {}
    
    if (option == "CW"):
        prefix = "mapCW"
    else:
        prefix = "mapWC"
        
    for i in range(0, int(numDiv)):
    
        name = prefix + str(i)
        #Get the dictionary
        d = cos.get_object("noobucket", name).decode('utf-8-sig')
        d = ast.literal_eval(d)
        #Delete the dictionary
        cos.delete_object("noobucket", name)
        
        #Check and upadte value in dictionary
        for key, value in d.items():
                
            if key in final_dict:
                finalValue = final_dict.get(key)
                finalValue += value
            else:
                finalValue = value
        
            final_dict.update({key : finalValue})
            
            
    #Order dictionary
    a = OrderedDict(sorted(final_dict.items(), key = lambda x: x[1]))
    
    #Upload the result dictionary
    cos.put_object("noobucket", resultName, str(dict(a)))
    
    #---------------------------------------------------------------------------------------
    
    channel.basic_publish(exchange='',
                      routing_key='reduce_queue',
                      body= resultName)
    
    connection.close()
    
    #---------------------------------------------------------------------------------------
    
    
    return final_dict