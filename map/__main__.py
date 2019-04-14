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
    
    #Instanciate cos
    cos = cos_backend.cos_backend(args)
    inter = {'Range' : 'bytes=' + start + '-' + fi}
    
    #Get partition
    text = cos.get_object("noobucket", name, extra_get_args=inter)
    #Decode binary to String
    text = text.decode('latin-1')
    
    #    Parse text --> replace punctuations with blanks
    # Define punctuation
    punctuations = '''!()-[]{};:'"\,<>.?@#$%^&*_~=\n\r\t'''
    #Substitute
    for char in punctuations:
        text = text.replace(char,' ')
    
    #--------------------------------------------------------------------------------------
    #Don't take the last word if it doesn't end in Blank
    last_char = text[-1:]
    
    if ( last_char != ' ' ):
        
        text = text.rsplit(' ', 1)[0]
    
    #Search word not taken by last partition (if it is not taken, not the first partition)
    start = int(start)
    
    if ( start != 0 ):
    
        new_word = ""
        start = start - 1
        
        #Start collecting previous word characters
        while ( True ):
            
            inter = {'Range' : 'bytes=' + str(start) + '-' + str(start)}
            #Get text file
            char_aux = cos.get_object("noobucket", name, extra_get_args=inter)
            #Decode binary to String
            char_aux = char_aux.decode('latin-1')
            
            
            if (punctuations.find(char_aux) == -1 and char_aux != ' '):
                
                new_word = char_aux + new_word
            #If its a blank or a punctuation sign stop collecting characters
            else:
                break;
            
            #Next character
            start = start - 1
        
        #Add new word to text
        text = new_word + text
    
    #Lower case
    text = text.lower()
    
    #Delete space key and split words
    words = filter(None, text.split(' '))
    
    #--------------------------------------------------------------------------------------
    #Let's count words
    d = {}
    if ( option == "WC"):
    
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
                value += 1
            else:
                value = 1
            
            d.update({ "word" : value})
    
    #Upload file with dictionary to COS
    cos.put_object("noobucket", result, str(d))
    
    #---------------------------------------------------------------------------------------
    
    channel.basic_publish(exchange='',
                      routing_key='map_queue',
                      body= result)
    
    connection.close()
    
    #---------------------------------------------------------------------------------------
    
    return {"done" : "done"}
