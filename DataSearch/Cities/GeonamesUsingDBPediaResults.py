
# coding: utf-8

# In[ ]:

import pandas as pd
import csv
import urllib

dfDBPediaCity = pd.read_csv('DBPediaCityReduced.csv', sep=',')
dfGeoNamesCity = pd.read_csv('GeonamesData.csv', sep=',')


# In[ ]:


import rdflib
from rdflib import URIRef
import pandas as pd

def convertResultToDataFrame( identifier ):
    try:                         
        g = rdflib.Graph()
        link = ''.join([identifier, 'about.rdf'])
        print(link)
        g.load(link)
                             
        cityGeo = {}
        cityGeo['URI'] = identifier
        
        searchedFeatures = ['parentCountry']
        easySearchedFeatures = ['lat', 'long', 'label', 'population']
        skipFeatures = ['featureClass', 'featureCode', 'isDefinedBy', 'locationMap', 'nearbyFeatures', 'type']
            
        for p,o in g.predicate_objects(URIRef(cityGeo['URI'])):
            if(type(o) is rdflib.term.Literal):
                if(p == URIRef('http://www.geonames.org/ontology#name')):
                    cityGeo['label'] = o.value
                else:
                    if hasattr(o, 'lang'):
                        if o.lang == 'en':
                            if p.split('#')[-1] in easySearchedFeatures:
                                cityGeo[p.split('#')[-1]] = o
                    elif p.split('#')[-1] in easySearchedFeatures:
                        cityGeo[p.split('#')[-1]] = o
            else:
                pLiteral = p.split('#')[-1]
                if(pLiteral in searchedFeatures):
                    try:
                        newG = rdflib.Graph()
                        newG.load(''.join([o, 'about.rdf']))
                        for newS, newP, newO in newG:
                            if(newP == URIRef('http://www.geonames.org/ontology#name')):
                                cityGeo[pLiteral] = newO.value
                                break
                    except:
                        print('New Error - Related Link')
                else:
                    if(pLiteral in easySearchedFeatures):
                        cityGeo[pLiteral] = o

    except ValueError:
        print('ValueError')
        return pd.DataFrame()
    except ConnectionResetError:
        print('ConnectionError')
        return pd.DataFrame()
    except urllib.error.HTTPError:
        print(identifier)
     #   print('New Error')
        return pd.DataFrame()
    return pd.DataFrame(cityGeo, index=[0])


counter = 0

for row in dfDBPediaCity['links']:
    for link in row.split('\t'):
        if 'http://sws.geonames.org/' in link and link not in dfGeoNamesCity['URI'].values:
            dfGeoNamesCity = dfGeoNamesCity.append(convertResultToDataFrame(link), ignore_index=True, sort=True)
            if counter == 10:
                dfGeoNamesCity.to_csv('GeonamesData.csv', sep=',', encoding='utf-8', index=False, quotechar='"', quoting=csv.QUOTE_ALL)
                counter = 0
            else:
                counter = counter + 1

# In[ ]:

dfGeoNamesCity.to_csv('GeonamesData.csv', sep=',', encoding='utf-8', index=False, quotechar='"', quoting=csv.QUOTE_ALL)

