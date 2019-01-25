import pandas as pd

import logging
import ujson
import gzip
import rdflib

title = {}
author = {}

def read_wiki_file(file_path):
    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
        element = ''
        rdfMeta = ''
        collect = False
        for i, line in enumerate(f):
            if i < 30 and i != 0:
                rdfMeta = rdfMeta + line

            element = element + line

            if line.startswith('</rdf:Description>'):
                element = element + '</rdf:RDF>'
                g = rdflib.Graph()

                g.parse(data=element, format="application/rdf+xml")

                for s, p, o in g:
                    if 'd-nb.info' in str(s):
                        if not 'owl#sameAs' in str(p):
                            print('---------------------------------')
                            print(s, p, o)

                element = rdfMeta



if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO)
    dfAuthors = pd.read_csv('DnbDataAuthorsReduced.csv', sep=',', low_memory=False)

    read_wiki_file('DNBTitel.rdf.gz')
