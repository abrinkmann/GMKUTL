import logging
import gzip
import csv
import pandas as pd
from urllib.parse import unquote

def read_wiki_file(file_path):
    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
        next(f)
        dfHotels = pd.read_csv('hotels_www.touristlink.com_combined.csv', sep=',', index_col=0, low_memory=False)
        #dfHotels = pd.DataFrame()
        hotel = {}

        preferred_urls = {'marriott': 'www.marriott.com/hotels/travel', 'www.lovetoescape.com' : 'www.lovetoescape.com/hotels/', 'www.benitalia.com': 'www.benitalia.com/en'}

        current_hotel = ''
        current_source = ''
        place = {}
        rating = {}
        counter = len(dfHotels)
        searched_values = ['<http://schema.org/PostalAddress/addressCountry>', '<http://schema.org/PostalAddress/postalCode>', '<http://schema.org/AggregateRating/bestRating>',
                           '<http://schema.org/Hotel/priceRange>', '<http://schema.org/AggregateRating/ratingValue>', '<http://schema.org/Hotel/aggregateRating>',
                           '<http://schema.org/PostalAddress/addressLocality>', '<http://schema.org/PostalAddress/streetAddress>', '<http://schema.org/Hotel/address>',
                           '<http://purl.org/dc/terms/title>', '<http://schema.org/Hotel/url>', '<http://schema.org/Hotel/name>',
                           '<http://www.w3.org/1999/xhtml/microdata#item>', '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>']


        for i, line in enumerate(f):
             reader = csv.reader([line], delimiter=' ', quotechar='"')
             try:
                for r in reader:
                    #print(r)

                    if len(r) > 3 and r[1] in searched_values and 'touristlink.com' in r[3]:
                        print(r)
                        #print(r)
                        collect = True
                        #logic to select sources
                        for key in preferred_urls:
                            if key in r[3]:
                                collect = False
                                if preferred_urls[key] in r[3]:
                                    collect = True

                        if collect:


                            if r[1] in hotel:
                                hotel[r[1]] = hotel[r[1]] + 1
                            else:
                                hotel[r[1]] = 1


                            if 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' in r[1] and 'http://schema.org/Hotel' in r[2]:
                                current_hotel = r[0]
                                current_source = r[3]
                                hotel['uri'] = current_hotel
                                hotel['source'] = current_source
                                hotel['source_reduced'] = current_source.replace('<', '').replace('>', '').replace('https://', '').replace('http://', '').replace('//', '').split('/')[0]

                            else:
                                if r[3] == current_source:
                                    if 'http://schema.org/Hotel/address' in r[1] and 'uri' in place and place['uri'] == r[2]:
                                        for key in place:
                                            new_key = key.replace('Hotel/', 'hotel_').replace('PostalAddress/', 'postaladdress_').replace('Place/', 'place_').lower()
                                            hotel[new_key] = unquote(place[key])

                                    elif 'http://schema.org/Hotel/aggregateRating' in r[1] and 'uri' in rating and rating['uri'] == r[2]:
                                        for key in rating:
                                            new_key = key.replace('Hotel/', 'hotel_').replace('PostalAddress/','postaladdress_').replace('Place/', 'place_').lower()
                                            hotel[new_key] = unquote(rating[key])

                                    elif r[0] == current_hotel:
                                        identifier = r[1].split('/')[-2] + "/" + r[1].split('/')[-1].replace('>', '')
                                        identifier = identifier.replace('Hotel/', 'hotel_').replace('PostalAddress/','postaladdress_').replace('Place/', 'place_').lower()
                                        hotel[identifier] = ' '.join(r[2].replace('\\t','').replace('\\n','').split())

                                    elif 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' in r[1] and 'http://schema.org/PostalAddress' in r[2]:
                                        place['uri'] = r[0]

                                    elif 'uri' in place and r[0] == place['uri']:
                                        identifier = r[1].split('/')[-2] + "/" +r[1].split('/')[-1].replace('>', '')
                                        identifier = identifier.replace('Hotel/', 'hotel_').replace('PostalAddress/', 'postaladdress_').replace('Place/', 'place_').lower()
                                        place[identifier] = ' '.join(r[2].replace('\\t','').replace('\\n','').split())

                                    elif 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' in r[1] and 'http://schema.org/AggregateRating' in r[2]:
                                        rating['uri'] = r[0]

                                    elif 'uri' in rating and r[0] == rating['uri']:
                                        identifier = r[1].split('/')[-2] + "/" +r[1].split('/')[-1].replace('>', '')
                                        identifier = identifier.replace('Hotel/', 'hotel_').replace('PostalAddress/','postaladdress_').replace('Place/', 'place_').lower()
                                        rating[identifier] = ' '.join(r[2].replace('\\t','').replace('\\n','').split())

                if 'http://www.w3.org/1999/xhtml/microdata#item' in r[1] and r[2] == current_hotel:
                    #print(hotel)
                    if not 'hotel_url' in hotel:
                        hotel['hotel_url'] = r[0]
                    if 'hotel_url' in hotel and 'hotel_name' in hotel and '@en' in hotel['hotel_name'] and (len(dfHotels) == 0 or ( not hotel['uri'] in dfHotels['uri'].values and not hotel['hotel_url'] in dfHotels['hotel_url'].values )):
                        #save_result = False
                        #print('Instance found!')
                        for key in hotel:
                            hotel[key] = unquote(str(hotel[key]).split('@en')[0], encoding='utf-8')

                        hotel['uri'] = 'revngo.com_' + str(counter)
                        counter = counter + 1
                        #for key in hotel:
                         #   if 'hilton' in hotel[key].lower():
                          #       save_result = True
                           # if 'london' in hotel[key].lower() and 'holiday' in hotel[key].lower():
                            #    save_result = False
                             #   break



                        #if save_result:
                        dfHotels = dfHotels.append(pd.DataFrame(hotel, index=[0]), sort=False)
                    hotel = {}

             except csv.Error as e:
                print(e)

             # if len(dfHotels) == 40000:
             #     print('Written to file!')
             #     dfHotels.to_csv('hotels_revngo.com_combined_v2.csv', sep=',', encoding='utf-8', index=False, quotechar='"', quoting=csv.QUOTE_ALL)

             # if len(dfHotels) > 10000:
             #     break
             # #     filename = 'new_ihg_' + str(counter) + '.csv'
             #     counter = counter + 1
             #     if hasattr(dfHotels, 'to_csv'):
             #        dfHotels.to_csv(filename, sep=',', encoding='utf-8', index=False, quotechar='"', quoting=csv.QUOTE_ALL)
             #     dfHotels = pd.DataFrame()
             #     print("Printed: " + filename)

        #filename = 'fragments/hotels_hilton_' + str(counter) + '.csv'
        dfHotels.to_csv('hotels_www.touristlink.com_combined_v2.csv', sep=',', encoding='utf-8', index=False, quotechar='"', quoting=csv.QUOTE_ALL)
        # print(pd.DataFrame(hotel, index=[0]).transpose().sort_values(by=[0], ascending=False))

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO)
    read_wiki_file('schema_Hotel.gz')

    #dfHotels.to_csv('hotels.csv', sep=',', encoding='utf-8', index=False, quotechar='"', quoting=csv.QUOTE_ALL)
