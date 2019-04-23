'''
Created on 12 abr. 2019

@author: aaroni34

Uploading file
'''

from src import cos_backend
import yaml

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

inter = {'Range' : 'bytes=' + str(0) + '-' + str(1024)}

#t = cos.get_object("noobucket", "gutenberg-1G.txt", extra_get_args = inter).decode('utf-8-sig')


f = open("gutenberg-1G.txt", "rb")
cos.put_object("noobucket", "gutenberg-1G.txt", f.read().decode('latin-1'))


