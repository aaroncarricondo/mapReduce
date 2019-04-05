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
    lineList = map(lambda s: s.replace('.',''), lineList)
    lineList = map(lambda s: s.replace(',',''), lineList)

    
    dicts = []
    
    for x in lineList:
        newdict = {}
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