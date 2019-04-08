'''
Created on 20 mar. 2019

@author: aaroni34
'''

def mapRed ():
    ###########
    #Read file
    
    f = open("pg2000.txt", "r")
    inputString = f.read()
    
    inputString = inputString.replace('.','')
    
    lineList = inputString.splitlines()
    #lineList = list(filter(None, lineList))
    #Delete punctuation signs
    # Define punctuation
    punctuations = '''!()-[]{};:'"\,<>./?@#$%^&*_~'''
    
    
    dicts = []
    Wdicts = []
    
    for x in lineList:
        newdict = {}
        
        #Substitute
        for char in punctuations:
            x = x.replace(char,'')
        
        punctuations = '\n\r\t'
        
        #Substitute
        for char in punctuations:
            x = x.replace(char,' ')
            
        #Delete space key
        lineWords = filter(None, x.split(' '))
        
        lineWords = x.split(' ')
        
        for word in lineWords:
            if word in newdict:
                value = newdict.get(word)
                value+=1
            else:
                value = 1
            
            newdict.update({word : value})
        
        dicts.append(newdict)
    
    finaldict = {}
    
    for x in dicts:
        for key, value in x.items():
            
            if key in finaldict:
                finalValue = finaldict.get(key)
                finalValue += value
            else:
                finalValue = value
        
            finaldict.update({key : finalValue})
    
    
    print(finaldict)
    

mapRed()

text = "Hola que  tal"
last_char = text[-1:]
print(last_char)
text.lower()
print(text)
if ( last_char != ' ' or last_char != '.' or last_char != ',' ):
        
        print("Entro")
        text = text.rsplit(' ', 1)[0]
        print("Salgo")

        
print (text)