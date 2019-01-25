import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
import csv

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
dfCity = pd.DataFrame()
#dfCity = pd.read_csv('DBPediaSettelmentReduced.csv', sep=',', encoding='utf-8')
print("Dataframe prepared!")
counter = -1
offset = 0

while (len(dfCity) != counter):
    counter = len(dfCity)
    sparql = SPARQLWrapper("http://dbpedia.org/sparql")
    query = """
        SELECT DISTINCT ?s ?label ?abstract ?long ?lat ?population ?populationMetro ?country
        WHERE
            {
            ?s a dbo:City.
            ?s rdfs:label ?label .
            ?s owl:sameAs ?links .
            OPTIONAL { ?s geo:long ?long }.
            OPTIONAL { ?s geo:lat ?lat }.
            OPTIONAL { ?s dbo:abstract ?abstract }.
            OPTIONAL { ?s dbo:country ?country }.
            OPTIONAL { ?s dbo:populationTotal ?population}.
            OPTIONAL { ?s dbo:populationMetro ?populationMetro}.
            FILTER (lang(?label) = 'en').
            FILTER (lang(?abstract) = 'en').
            FILTER (regex(str(?links), "http://sws.geonames.org") )
            }
            LIMIT 10000 OFFSET %d
            
    """ % offset
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    # In[26]:
    for result in results["results"]["bindings"]:
        city = {}
        if 'URI' not in dfCity or result['s']['value'] not in dfCity['URI'].values:
            if result['label']['xml:lang'] == 'en':
                city['URI'] = result['s']['value']
                city['label'] = result['label']['value']
                if 'long' in result:
                    city['long'] = result['long']['value']
                if 'lat' in result:
                    city['lat'] = result['lat']['value']
                if 'country' in result:
                    city['country'] = result['country']['value'].replace('http://dbpedia.org/resource/', '')
                if 'population' in result:
                    city['population'] = result['population']['value']
                if 'populationMetro' in result:
                    city['populationMetro'] = result['populationMetro']['value']
                if 'abstract' in result:
                    city['abstract'] = result['abstract']['value'].replace('\n','').replace('\r', '').replace('"','')
                dfNewCity = pd.DataFrame(city, index=[0])
                dfCity = dfCity.append(dfNewCity, ignore_index=True, sort=True)
    offset += 10000
    print("City length %d" % len(dfCity))
    dfCity.to_csv('DBPediaCityReduced.csv', sep=',', encoding='utf-8', index=False, quotechar='"',
                  quoting=csv.QUOTE_ALL)



#dfCity = pd.read_csv('DBPediaCitiesReduced2.0.csv', sep=',', encoding='utf-8')
print("Finished to collect cities")

dfCityWithLinks = pd.DataFrame()
for index, row in dfCity.iterrows():
        sparql = SPARQLWrapper("http://dbpedia.org/sparql")
        query = """SELECT DISTINCT ?links WHERE {  
                <%s>  owl:sameAs ?links. 
                FILTER (regex(str(?links), "http://sws.geonames.org") || regex(str(?links), "http://www.wikidata.org"))
                } 
                LIMIT 10""" % row['URI']
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        links = ''
        if len(results["results"]["bindings"]) > 0:
            for result in results["results"]["bindings"]:
                links = links + '\t' + result['links']['value']
            row['links'] = links
            dfCityWithLinks = dfCityWithLinks.append(row)

print("Finished to collect same as Links!")

print("%d cities collected" % len(dfCityWithLinks))

dfCityWithLinks.to_csv('DBPediaCityReduced.csv', sep=',', encoding='utf-8', index=False, quotechar='"', quoting=csv.QUOTE_ALL)