
# coding: utf-8

# In[ ]:


import pandas as pd

dfCities = pd.read_csv('DBPediaCity.csv', sep=',', index_col=0, low_memory=False)
dfCityWikiData = pd.DataFrame()
#dfCityWikiData = pd.read_csv('WikiDataCity.csv', sep=',', index_col=0, low_memory=False)

#display(dfCities.head())
display(dfCityWikiData.head())


# In[ ]:


import rdflib
from rdflib import URIRef
import pandas as pd
import urllib3
import json
import csv

http = urllib3.PoolManager()
urllib3.disable_warnings()

dict_lookup = {}

def lookUpProperty(identifier):
    
    if identifier in dict_lookup:
        return dict_lookup[identifier]
    
    url = "https://www.wikidata.org/wiki/Special:EntityData/%s.json" % identifier
    r = http.request('GET', url)
    
    data = json.loads(r.data.decode('utf-8'))
    json_entity = data['entities'][identifier]
    
    if 'en' in json_entity['labels']:
        dict_lookup[identifier] = json_entity['labels']['en']['value']
        return json_entity['labels']['en']['value']
    
    dict_lookup[identifier] = None
    return None


def convertResultToDataFrame( identifier ):
    try:
        url = "https://www.wikidata.org/wiki/Special:EntityData/%s.json" % identifier
        r = http.request('GET', url)
    
        data = json.loads(r.data.decode('utf-8'))
        #display(identifier)
        #display(data)
        if identifier in data['entities']:
            json_entity = data['entities'][identifier]
    
            result = {}
            result['uri'] = 'http://www.wikidata.org/entity/' + identifier
            if 'en' in json_entity['labels']:
                result['label'] = json_entity['labels']['en']['value']
    
                for claim in json_entity['claims']:
                    prop = lookUpProperty(claim)
                    if prop != None:
                        for obj in json_entity['claims'][claim]:
                            if 'mainsnak' in obj and 'datavalue' in obj['mainsnak']:
                        #if obj['mainsnak']['datavalue']['type'] == 'string':
                         #   if prop in result:
                          #      result[prop] = ''.join([result[prop], '\t', obj['mainsnak']['datavalue']['value']])
                           # else:
                            #    result[prop] = obj['mainsnak']['datavalue']['value']
                                if obj['mainsnak']['datavalue']['type'] == "wikibase-entityid":
                                    value = lookUpProperty(obj['mainsnak']['datavalue']['value']['id'])
                                    if value != None:
                                        if prop in result:
                                            result[prop] = ''.join([result[prop], '\t', value])
                                        else:
                                            result[prop] = value
                                elif obj['mainsnak']['datavalue']['type'] == "time":
                                    if prop in result:
                                        result[prop] = ''.join([result[prop], '\t', obj['mainsnak']['datavalue']['value']['time']])
                                    else:
                                        result[prop] = obj['mainsnak']['datavalue']['value']['time']
                        #else:
                         #   display(prop)
                          #  display(obj['mainsnak']['datavalue']['value'])
                return pd.DataFrame(result, index=[0])
    except Exception as e:
        display(e)
        display(identifier)
        return pd.DataFrame()
    
    return pd.DataFrame()


counter = 0
for row in dfCities['owl#sameAs']:
    for link in row.split('\t'):
        if 'http://www.wikidata.org/entity/' in link:
            #if link not in dfCityWikiData['uri'].values:
                dfCityWikiData = dfCityWikiData.append(convertResultToDataFrame(link.split('/')[-1]), ignore_index=True, sort=True)
                if counter == 10:
                    dfCityWikiData.to_csv('WikiDataCityNew.csv', sep=',', encoding='utf-8', index=False, quotechar='"', quoting=csv.QUOTE_ALL)
                    counter = 0
                else:
                    counter = counter + 1

display(dfCityWikiData)


# In[ ]:


import rdflib
from rdflib import URIRef
import pandas as pd
import decimal
import urllib

def lookUpLiteral(identifier, g):
    for p,o in g.predicate_objects(URIRef('http://www.wikidata.org/entity/' + identifier)):
        if(type(o) is rdflib.term.Literal):
                if(o.language == 'en' and p == URIRef('http://schema.org/name')):
                        return o.value

def lookUpCoordiante(identifier, g):
    for p,o in g.predicate_objects(URIRef('http://www.wikidata.org/entity/statement/' + identifier)):
        if(type(o) is rdflib.term.Literal):
            return str(o)
        
def lookUpPopulation(identifier, g):
    for p,o in g.predicate_objects(URIRef('http://www.wikidata.org/entity/statement/' + identifier)):
        if(type(o) is rdflib.term.Literal and o.datatype == URIRef('http://www.w3.org/2001/XMLSchema#decimal')):
            return o.value

def convertResultToDataFrame( identifier ):
    try:                         
        g = rdflib.Graph()
        g.load(''.join(['https://www.wikidata.org/wiki/Special:EntityData/', identifier]))
                             
        city = {}
        city['URI'] = ''.join(['http://www.wikidata.org/entity/', identifier])
        #display('_______________________')

        for p,o in g.predicate_objects(URIRef(city['URI'])):
            
            if(type(o) is rdflib.term.Literal):
                if(o.language == 'en' and p == URIRef('http://schema.org/name')):
                    city['label'] = o.value
            else:
                if 'wikidata.org' in p:
                    pLiteral = lookUpLiteral(p.split('/')[-1], g)
                else:
                    pLiteral = p
                if 'wikidata.org' in o:
                    oLiteral = lookUpLiteral(o.split('/')[-1], g)
                    if str(p) ==  'http://www.wikidata.org/prop/P625':
                        oLiteral = lookUpCoordiante(o.split('/')[-1], g)
                    if str(p) ==  'http://www.wikidata.org/prop/P1082':
                        oLiteral = lookUpPopulation(o.split('/')[-1], g)
                else:
                    oLiteral = o
                if(pLiteral != None and oLiteral != None):
                    if(pLiteral in city):
                        if type(oLiteral) == decimal.Decimal:
                            city[pLiteral] = oLiteral
                        else:
                            city[pLiteral] = '\t '.join([city[pLiteral], oLiteral])
                    else:
                        city[pLiteral] = oLiteral
    except ValueError:
        display('Value Error')
        display(identifier)
        return pd.DataFrame()
    except ConnectionResetError:
        display(identifier)
        return pd.DataFrame()
    except urllib.error.HTTPError:
        display('HTTPError')
        display(''.join(['https://www.wikidata.org/wiki/Special:EntityData/', identifier]))
        return pd.DataFrame()
   # except:
    #    display('Value Error')
     #   display(identifier)
      #  return pd.DataFrame()
    return pd.DataFrame(city, index=[0])

#counter = 0
#for row in dfCities['owl#sameAs']:
#    for link in row.split('\t'):
 #       if 'http://www.wikidata.org/entity/' in link:
 #           if link not in dfCityWikiData['URI'].values:
  #              dfCityWikiData = dfCityWikiData.append(convertResultToDataFrame(link.split('/')[-1]), ignore_index=True, sort=True)
   #             if counter == 10:
    #                #dfCityWikiDataMeta = dfCityWikiData.describe()
     #               #dfCityWikiDataMetaCount = dfCityWikiDataMeta.loc[['count']]
      #              #dfCityWikiDataMetaCount = dfCityWikiDataMetaCount.transpose()
       #             #dfSelected = dfCityWikiDataMetaCount[((dfCityWikiDataMetaCount['count']/len(dfCityWikiDataMetaCount)) * 100) > 15]
        ##            #dfCityWikiData2 = dfCityWikiData[dfSelected.index]
         #           dfCityWikiData.to_csv('WikiDataDataCities2pre.csv', sep=';')
          #          counter = 0
           #     else:
            #        counter = counter + 1

#display(dfCityWikiData)


# In[ ]:


dfCityWikiData.to_csv('WikiDataCityNew.csv', sep=',', encoding='UTF-8', index=None)

