import pandas as pd

dfHotelsCom= pd.read_csv('hotels_www.touristlink.com.csv', sep=',')
dfRevngo = pd.read_csv('hotels_revngo.com.csv', sep=',', low_memory=False)

dfGS = pd.read_csv('goldstandard_revngo_www.touristlink.com.csv', sep=',', index_col=None, header=None)
#dfGS = pd.DataFrame()
dfCorrespondences = pd.read_csv('correspondences/knowledgebase_hotel_2_hotels_www.touristlink.com_correspondences_label.csv', sep=',', header=None)

found_instances = len(dfGS)
dfCorrespondences = dfCorrespondences.sort_values(by=[1])
dfCorrespondences = dfCorrespondences.sort_values(by=[2], ascending=False)

for index, row in dfCorrespondences.iterrows():
    if (len(dfGS) == 0 ) or ( not row[0] in dfGS[0].values and not row[1] in dfGS[1].values):
        print(str(found_instances) + ' instances found!')
        dfSubsetRevngo = dfRevngo[dfRevngo['uri'] == row[0]].iloc[0]
        print(dfSubsetRevngo.values)

        print('_________________________')

        dfSubsetHotelsCom = dfHotelsCom[dfHotelsCom['uri'] == row[1]].iloc[0]
        print(dfSubsetHotelsCom.values)

        answer = input("t/f?")
        if answer == 't':
            row[2] = 'TRUE'
            found_instances = found_instances + 1
            dfGS = dfGS.append(row)
            dfGS.to_csv('goldstandard_revngo_www.touristlink.com.csv', sep=',', index=None, header=None)

        print('_________________________')
        print('_________________________')


#print(dfDesignmyNight)
#print(dfCarpediem)
#print(dfCorrespondences)