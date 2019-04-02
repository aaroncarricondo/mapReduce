'''
Created on 24 mar. 2019

@author: aaroni34
'''

import cos_backend

def main(args):
    start = str(args["start"])
    fi = str(args["fi"])
    name = args["fileName"]
    result = args["resultName"]
    #COS parameters
    endpoint = args["endpoint"]
    secret_key = args["secret_key"]
    access_key = args["access_key"]
    
    #Instanciate cos
    cos = cos_backend.cos_backend({ "endpoint" : endpoint, "secret_key" : secret_key, "access_key" : access_key})
    inter = {'Range' : 'bytes=' + start + '-' + fi}
    
    text = cos.get_object("noobucket", name, extra_get_args=inter)
    #Decode binary to String
    text = text.decode('utf-8-sig')
    
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
    
    for word in words:
        
        if ("word") in d:
            value = d.get("word")
        else:
            value = 1
        
        d.update({ "word" : value + 1})
    
    #Upload file with dictionary to COS
    cos.put_object("noobucket", result, str(d))
    
    return d