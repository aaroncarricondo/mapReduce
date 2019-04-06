'''
Created on 24 mar. 2019

@author: aaroni34
'''

import cos_backend
import pika, os

def main(args):
    start = str(args["start"])
    fi = str(args["fi"])
    name = args["fileName"]
    result = args["resultName"]
    option = args["option"]
    rabbit = str(args['ibm_rabbit'])
    
    #--------------------------------------------------------------------------------------
    # Access the CLODUAMQP_URL environment variable and parse it (fallback to localhost)
    url = os.environ.get('CLOUDAMQP_URL', rabbit)
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel() # start a channel
    channel.queue_declare(queue='map_queue', durable=True, exclusive=False, auto_delete=False) # Declare a queue
        
    #---------------------------------------------------------------------------------------
    
    #COS parameters
    endpoint = args["endpoint"]
    secret_key = args["secret_key"]
    access_key = args["access_key"]
    
    #Instanciate cos
    cos = cos_backend.cos_backend({ "endpoint" : endpoint, "secret_key" : secret_key, "access_key" : access_key})
    inter = {'Range' : 'bytes=' + start + '-' + fi}
    
    #Take first word if its broken
    if (start == '0'):
        text = cos.get_object("noobucket", name, extra_get_args=inter)
        #Decode binary to String
        text = text.decode('utf-8-sig')
        
    else:
        
        start = int(start)
        
        while ( True ):
            inter = {'Range' : 'bytes=' + str(start) + '-' + fi}
            #Get text file
            text = cos.get_object("noobucket", name, extra_get_args=inter)
            #Decode binary to String
            text = text.decode('utf-8-sig')
            
            first_char = text[:1]
            if (first_char == ' '):
                break
            
            start = start - 1
    
    
    #------------------------------------
    #Delete punctuation signs
    # Define punctuation
    punctuations = '''!()-[]{};:'"\,<>./?@#$%^&*_~'''
    #Substitute
    for char in punctuations:
        text = text.replace(char,'')
    
    punctuations = '\n\r\t'
    
    #Substitute
    for char in punctuations:
        text = text.replace(char,' ')
        
    #Delete space key
    words = filter(None, text.split(' '))
    #------------------------------------
    #Let's count words
    d = {}
    if ( option == "CW"):
    
        for word in words:
            
            if word in d:
                value = d.get(word)
                value+=1
            else:
                value = 1
                
            d.update({word : value})
    
    else:
        
        for word in words:
        
            if ("word") in d:
                value = d.get("word")
            else:
                value = 1
            
            d.update({ "word" : value + 1})
            
    #Upload file with dictionary to COS
    cos.put_object("noobucket", result, str(d))
    
    #---------------------------------------------------------------------------------------
    
    channel.basic_publish(exchange='',
                      routing_key='map_queue',
                      body= result)
    
    connection.close()
    
    #---------------------------------------------------------------------------------------

    
    return d
