'''
Created on 31 mar. 2019

@author: aaroni34
'''
import cos_backend
import ast

def main(args):
    #Get parameters
    numDiv = args["numDiv"]
    resultName = args["resultName"]
    
    #Instantiate COS
    #COS parameters
    endpoint = args["endpoint"]
    secret_key = args["secret_key"]
    access_key = args["access_key"]
    
    #Instanciate cos
    cos = cos_backend.cos_backend({ "endpoint" : endpoint, "secret_key" : secret_key, "access_key" : access_key})
    
    final_dict = {}
    
    for i in range(0, int(numDiv)):
    
        name = "map" + str(i)
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
    
    #Upload the result dictionary
    cos.put_object("noobucket", resultName, str(final_dict))
    
    
    return final_dict