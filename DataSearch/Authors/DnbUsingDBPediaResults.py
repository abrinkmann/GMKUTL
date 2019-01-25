
# coding: utf-8

# In[1]:


import pandas as pd

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

dfAuthors = pd.read_csv('DBPediaAuthorsReduced.csv', sep=',', low_memory=False)
dfAuthorDnB = pd.read_csv('DnbDataAuthorsReduced.csv', sep=',', low_memory=False)

import rdflib
from rdflib import URIRef
import pandas as pd
import csv


def convertResultToDataFrame( identifier ):                        
    try:
        g = rdflib.Graph()
        g.load(''.join([identifier, '/about/rdf']))
                             
        author = {}
        author['uri'] = identifier
        
        searchedFeatures = ['gender', 'preferredNameEntityForThePerson', 'dateOfBirth', 'dateOfDeath', 'preferredNameForThePerson']
                    
        
        for p,o in g.predicate_objects(URIRef(author['uri'])):
            pLiteral = p.split('#')[-1]
            if pLiteral in searchedFeatures:
                if pLiteral == 'gender':
                    author[pLiteral] = o.split('#')[-1] 
                
                if type(o) is rdflib.term.Literal:
                    object1 = o.split('#')[-1]
                    if(pLiteral in author):
                        author[pLiteral] = '\t '.join([author[pLiteral], object1])
                    else:
                        author[pLiteral] = object1
                
                if 'preferredNameEntityForThePerson' in pLiteral:
                    for pEntity, oEntity in g.predicate_objects(o):
                        author[pEntity.split('#')[-1]] = oEntity.split('#')[-1]
                else:
                    author[pLiteral] = o.split('#')[-1].replace('\n', '')
        return pd.DataFrame(author, index=[0])

    except Exception as e:
        print(e)
        return pd.DataFrame()

counter = 0
for row in dfAuthors['links']:
    for link in row.split('\t'):
        if 'http://d-nb.info/gnd/' in link:
            if link not in dfAuthorDnB['uri'].values:
                dfAuthorDnB = dfAuthorDnB.append(convertResultToDataFrame(link), ignore_index=True, sort=True)
                if counter == 10:
                    dfAuthorDnB.to_csv('DnbDataAuthorsReduced.csv', sep=',', encoding='utf-8', index=False, quotechar='"', quoting=csv.QUOTE_ALL)
                    counter = 0
                else:
                    counter = counter + 1

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
dfAuthorDnB.to_csv('DnbDataAuthorsReduced.csv', sep=',', encoding='utf-8', index=False, quotechar='"', quoting=csv.QUOTE_ALL)

