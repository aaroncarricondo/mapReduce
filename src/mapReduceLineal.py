'''
Created on 20 mar. 2019

@author: aaroni34
'''

def mapRed ():
    ###########
    #Read file
    
    f = open("pg2000.txt", "rb")
    inputString = f.read().decode('utf-8-sig')
    
    inputString = inputString.replace('.','')
    
    lineList = inputString.splitlines()
    #lineList = list(filter(None, lineList))
    #Delete punctuation signs
    # Define punctuation
    punctuations = '''!()-[]{};:'"\,<>.=?@#$%^&*_~\n\r\t'''
    
    
    dicts = []
    dicts2 = []
    
    for x in lineList:
        newdict = {}
        newdict2 = {}
        #Substitute
        for char in punctuations:
            x = x.replace(char,' ')
        
        #Delete space key
        lineWords = filter(None, x.split(' '))
        
        
        for word in lineWords:
            if word in newdict:
                value = newdict.get(word)
                value+=1
            else:
                value = 1
            
            if "word" in newdict2:
                value2 = newdict2.get("word")
                value2 += 1
            else:
                value2 = 1
            
            newdict2.update({"word" : value2})

            newdict.update({word : value})
        
        dicts.append(newdict)
        dicts2.append(newdict2)
    
    finaldict = {}
    finaldict2 = {}
    
    for x in dicts:
        for key, value in x.items():
            
            if key in finaldict:
                finalValue = finaldict.get(key)
                finalValue += value
            else:
                finalValue = value
        
            finaldict.update({key : finalValue})
    
    for x in dicts2:
        for key, value in x.items():
            
            if key in finaldict2:
                finalValue = finaldict2.get(key)
                finalValue += value
            else:
                finalValue = value
        
            finaldict2.update({key : finalValue})
    
    
    print(finaldict)
    print(finaldict2)
    

mapRed()

punc = '''!)]}-;:'",>.?'''
char = 'm'

if (punc.find(char) == -1):
    print("YEs")