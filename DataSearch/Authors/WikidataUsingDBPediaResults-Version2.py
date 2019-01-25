
# coding: utf-8

# In[3]:


import pandas as pd
import urllib3
import json
import csv

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

dfAuthors = pd.read_csv('DBPediaAuthorsReduced.csv', sep=',', low_memory=False)
dfAuthorsWikiData = pd.read_csv('WikiDataAuthorsReduced.csv', sep=',', low_memory=False)

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
        return json_entity['labels']['en']['value'].strip()
    
    dict_lookup[identifier] = None
    return None


def convertResultToDataFrame( identifier ):
    try:
        url = "https://www.wikidata.org/wiki/Special:EntityData/%s.json" % identifier
        r = http.request('GET', url)
    
        data = json.loads(r.data.decode('utf-8'))
        prop_list = ['date of birth', 'date of death', 'notable work', 'sex or gender']

        if identifier in data['entities']:
            json_entity = data['entities'][identifier]
    
            author = {}
            author['uri'] = 'http://www.wikidata.org/entity/' + identifier
            if 'en' in json_entity['labels']:
                author['label'] = json_entity['labels']['en']['value']
    
                for claim in json_entity['claims']:
                    prop = lookUpProperty(claim)
                    if prop != None and prop in prop_list:
                        for obj in json_entity['claims'][claim]:
                            if 'mainsnak' in obj and 'datavalue' in obj['mainsnak']:
                                if obj['mainsnak']['datavalue']['type'] == "wikibase-entityid":
                                    value = lookUpProperty(obj['mainsnak']['datavalue']['value']['id'])
                                    if value != None:
                                        if prop in author:
                                            author[prop] = ''.join([author[prop], '\t', value])
                                        else:
                                            author[prop] = value
                                elif obj['mainsnak']['datavalue']['type'] == "time":
                                    if prop not in author:
                                        author[prop] = obj['mainsnak']['datavalue']['value']['time']
                return pd.DataFrame(author, index=[0])
    except Exception as e:
        print(e)
        return pd.DataFrame()
    
    return pd.DataFrame()

counter = 0
for row in dfAuthors['links']:
    for link in row.split('\t'):
        if 'http://www.wikidata.org/entity/' in link:
            if link not in dfAuthorsWikiData['uri'].values:
                dfAuthorsWikiData = dfAuthorsWikiData.append(convertResultToDataFrame(link.split('/')[-1]), ignore_index=True, sort=True)
                if counter == 10:
                    dfAuthorsWikiData.to_csv('WikiDataAuthorsReduced.csv', sep=',', encoding='utf-8', index=False, quotechar='"', quoting=csv.QUOTE_ALL)
                    counter = 0
                else:
                    counter = counter + 1

dfAuthorsWikiData.to_csv('WikiDataAuthorsReduced.csv', sep=',', encoding='utf-8', index=False, quotechar='"', quoting=csv.QUOTE_ALL)

