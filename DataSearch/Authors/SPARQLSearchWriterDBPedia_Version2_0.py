import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
import csv

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
dfAuthor = pd.DataFrame()
print("Dataframe prepared!")

#Add Meta Data - Run only once.
#Add meta data
author={}
author['URI'] = 'URI'
author['label'] = 'http://www.w3.org/2000/01/rdf-schema#label'
author['birthDate'] = 'http://dbpedia.org/ontology/birthDate'
author['gender'] = 'http://xmlns.com/foaf/0.1/gender'
author['deathDate'] = 'http://dbpedia.org/ontology/deathDate'
author['abstract'] = 'http://dbpedia.org/ontology/abstract'
dfNewAuthor = pd.DataFrame(author, index=[0])
dfAuthor = dfAuthor.append(dfNewAuthor, ignore_index=True, sort=True)

#Add meta data - datatypes
author={}
author['URI'] = 'URI'
author['label'] = 'XMLSchema#string'
author['birthDate'] = 'XMLSchema#date'
author['gender'] = 'XMLSchema#string'
author['deathDate'] = 'XMLSchema#date'
author['abstract'] = 'XMLSchema#string'
dfNewAuthor = pd.DataFrame(author, index=[0])
dfAuthor = dfAuthor.append(dfNewAuthor, ignore_index=True, sort=True)

#Add meta data - datatypes
author={}
author['URI'] = 'http://www.w3.org/2002/07/owl#Thing'
author['label'] = 'http://www.w3.org/2001/XMLSchema#string'
author['birthDate'] = 'http://www.w3.org/2001/XMLSchema#date'
author['gender'] = 'http://www.w3.org/2001/XMLSchema#string'
author['deathDate'] = 'http://www.w3.org/2001/XMLSchema#date'
author['abstract'] = 'http://www.w3.org/2001/XMLSchema#string'
dfNewAuthor = pd.DataFrame(author, index=[0])
dfAuthor = dfAuthor.append(dfNewAuthor, ignore_index=True, sort=True)



cols = dfAuthor.columns.tolist()
cols = cols[-1:] + cols[:-1]

dfAuthor = dfAuthor[cols]
counter = 0
offset = 0

while (len(dfAuthor) != counter):
    counter = len(dfAuthor)
    sparql = SPARQLWrapper("http://dbpedia.org/sparql")
    query = """
        SELECT DISTINCT ?s ?label ?gender ?birthDate ?deathDate ?abstract
        WHERE
            {
            ?s a dbo:Writer .
            ?s a foaf:Person .
            ?s rdfs:label ?label . 
            ?s owl:sameAs ?links . 
            OPTIONAL { ?s foaf:gender ?gender }.
            OPTIONAL { ?s dbo:birthDate ?birthDate.}.
            OPTIONAL { ?s dbo:deathDate ?deathDate }.
            OPTIONAL { ?s dbo:abstract ?abstract }.
            FILTER (lang(?label) = 'en')
            FILTER (lang(?abstract) = 'en')
            FILTER (lang(?gender) = 'en')
             
            }
            LIMIT 10000 OFFSET %d
    """ % offset
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    # In[26]:
    for result in results["results"]["bindings"]:
        author = {}
        if result['s']['value'] not in dfAuthor['URI'].values:
            if result['label']['xml:lang'] == 'en':
                author['URI'] = result['s']['value']
                author['label'] = result['label']['value']
                if 'gender' in result:
                    author['gender'] = result['gender']['value']
                if 'birthDate' in result:
                    author['birthDate'] = result['birthDate']['value']
                if 'deathDate' in result:
                    author['deathDate'] = result['deathDate']['value']
                if 'abstract' in result:
                    author['abstract'] = result['abstract']['value']
                dfNewAuthor = pd.DataFrame(author, index=[0])
                dfAuthor = dfAuthor.append(dfNewAuthor, ignore_index=True, sort=True)
    offset += 10000
    print("Author length %d" % len(dfAuthor))


dfAuthor.to_csv('DBPediaAuthorsReduced2.0.csv', sep=',', encoding='utf-8', index=False, quotechar='"', quoting=csv.QUOTE_ALL)
print("Finished to collect authors")

dfAuthorWithWork = pd.DataFrame()
for index, row in dfAuthor.iterrows():
    if index == 0:
        row['work'] = 'http://dbpedia.org/ontology/work'
        dfAuthorWithWork = dfAuthorWithWork.append(row)
    if index == 1:
        row['work'] = 'XMLSchema#list'
        dfAuthorWithWork = dfAuthorWithWork.append(row)
    if index == 2:
        row['work'] = 'https://www.w3.org/2001/XMLSchema#list'
        dfAuthorWithWork = dfAuthorWithWork.append(row)
    if index > 2:
        sparql = SPARQLWrapper("http://dbpedia.org/sparql")
        query = """SELECT DISTINCT * WHERE { { <%s> dbo:notableWork ?s. } UNION { ?s dbo:author <%s> . } } LIMIT 10000""" % (row['URI'], row['URI'])
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        work = ''
        if len(results["results"]["bindings"]) > 0:
            for result in results["results"]["bindings"]:
                work = work + '\t' + result['s']['value'].replace('http://dbpedia.org/resource/', '')
            row['work'] = work
        dfAuthorWithWork = dfAuthorWithWork.append(row)

print("Finished to collect list of works!")
dfAuthorWithWork.to_csv('DBPediaAuthorsReduced2.0.csv', sep=',', encoding='utf-8', index=False, quotechar='"', quoting=csv.QUOTE_ALL)

dfAuthorWithLinks = pd.DataFrame()
for index, row in dfAuthorWithWork.iterrows():
    if index == 0:
        row['links'] = 'www.w3.org/2002/07/owl#sameAs'
        dfAuthorWithLinks = dfAuthorWithLinks.append(row)
    if index == 1:
        row['links'] = 'XMLSchema#list'
        dfAuthorWithLinks = dfAuthorWithLinks.append(row)
    if index == 2:
        row['links'] = 'https://www.w3.org/2001/XMLSchema#list'
        dfAuthorWithLinks = dfAuthorWithLinks.append(row)
    if index > 2:
        sparql = SPARQLWrapper("http://dbpedia.org/sparql")
        query = """SELECT DISTINCT ?links WHERE {  <%s>  owl:sameAs ?links. } LIMIT 10000""" % row['URI']
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        links = ''
        if len(results["results"]["bindings"]) > 0:
            for result in results["results"]["bindings"]:
                links = links + '\t' + result['links']['value']
            row['links'] = links
        dfAuthorWithLinks = dfAuthorWithLinks.append(row)

print("Finished to collect same as Links!")

print("%d authors collected" % len(dfAuthorWithLinks))

# Add logic to collect sameAs link
# s

dfAuthorWithLinks.to_csv('DBPediaAuthorsReduced2.0.csv', sep=',', encoding='utf-8', index=False, quotechar='"', quoting=csv.QUOTE_ALL)