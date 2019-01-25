import csv
import pandas as pd

dfAuthors = pd.read_csv('DBPediaAuthorsReduced.csv', sep=',', low_memory=False)

dfAuthorsViaf = pd.read_csv('VIAFDataAuthorsReduced.csv', sep=',', low_memory=False)



import rdflib
import pandas as pd
import urllib.error as error
import numpy as np

import re

def getWorkTitle(identifier):
    try:
        g = rdflib.Graph()
        g.load(''.join([identifier, '/rdf.xml']))
    
        for s,p,o in g:
            if(str(p) == 'http://schema.org/name'):
                return str(o)

    except error.HTTPError:
        print(identifier)
        return None
        
    

def convertResultToDataFrame( identifier ):
        g = rdflib.Graph()
        g.load(''.join([identifier, '/rdf.xml']))
                             
        author = {}
        author['uri'] = identifier
        search_attributes = ['birthDate', 'deathDate', 'name', 'gender', 'notableWork']
        gender = {}
        gender['http://www.wikidata.org/entity/Q6581097'] = 'male'
        gender['http://www.wikidata.org/entity/Q6581072'] = 'female'
        gender['http://www.wikidata.org/entity/Q1097630'] = 'intersex'
        
        
        for s,p,o in g:
            predicate = p.split('/')[-1].strip()
            if predicate in search_attributes:
                add = False
            
                if type(o) is rdflib.term.Literal and not type(o.datatype) is rdflib.term.URIRef:
                    tmp_value = re.sub('\W', '', o.value.strip())
                    if  len(re.findall('[\0-\200]', tmp_value)) != 0 :
                        value = o.value
                        add = True
                else:
                    value = o
                    add = True
            
                if add:
                    
                    if predicate == 'notableWork' :
                        work = getWorkTitle(value)
                        if work != None:
                            if predicate not in author:
                                author[predicate] = work
                            elif(not str(work) in author[predicate].split('\t ')):
                                author[predicate] = '\t '.join([author[predicate], work])
                            
                    elif predicate not in author:
                        if predicate == 'gender':
                            author[predicate] = gender[str(value)]
                        else:
                            author[predicate] = value.replace('\n', '')
        return pd.DataFrame(author, index=[0])


counter = 0
for row in dfAuthors['links']:
    for link in row.split('\t'):
        if 'http://viaf.org/' in link and link not in dfAuthorsViaf['uri'].values:
            dfAuthorsViaf = dfAuthorsViaf.append(convertResultToDataFrame(link), ignore_index=True, sort=True)
            if counter == 10:
                dfAuthorsViaf.to_csv('VIAFDataAuthorsReduced.csv', sep=',', encoding='utf-8', index=False, quotechar='"', quoting=csv.QUOTE_ALL)
                counter = 0
            else:
                counter = counter + 1

dfAuthorsViaf.to_csv('VIAFDataAuthorsReduced.csv', sep=',', encoding='utf-8', index=False, quotechar='"', quoting=csv.QUOTE_ALL)