import csv
import sys
import os
from datetime import datetime
import requests
import urllib.request
import pandas
from sympy import python




#make directory for saving csvs
args=sys.argv
atm=datetime.now().strftime("%d-%m-%Y %H-%M-%S")
os.mkdir(atm)

#read Isins from csv file
with open(args[1]) as datapoints:
    datapoints_read = csv.reader(datapoints,delimiter=" ")
    point_list = list(datapoints_read)

#for every isin in csv fil
first=True;
masterdf=pandas.DataFrame();
for i in point_list[0]:
    #backgrund things for get request
    cookies = {
        'BIGipServer~OEKB~POOL_my.oekb.at_kapitalmarkt-services_https': '1326642575.5410.0000',
        'TS01deb77b': '010780ef504b7f483da860f87289d76c2946a11e45e43dbf016b8254ebf5b6476d56ae19afbc9b7aa6c0122b90fa4c79c797efb158',
        'BIGipServer~OEKB~POOL_my.oekb.at_kupl-services_rest_https': '1293088143.13090.0000',
        'BIGipServer~OEKB~POOL_kupl-kms.oekb.at_service_https': '1293088143.13090.0000',
    }

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) Gecko/20100101 Firefox/98.0',
        'Accept': 'application/json',
        'Accept-Language': 'de',
        # 'Accept-Encoding': 'gzip, deflate, br',
        'OeKB-Platform-Context': 'eyJsYW5ndWFnZSI6ImRlIiwicGxhdGZvcm0iOiJLTVMiLCJkYXNoYm9hcmQiOiJLTVNfT1VUUFVUIn0=',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Referer': 'https://my.oekb.at/kapitalmarkt-services/kms-output/fonds-info/sd/af/f?isin='+i,
        # Requests sorts cookies= alphabetically
        # 'Cookie': 'BIGipServer~OEKB~POOL_my.oekb.at_kapitalmarkt-services_https=1326642575.5410.0000; TS01deb77b=010780ef504b7f483da860f87289d76c2946a11e45e43dbf016b8254ebf5b6476d56ae19afbc9b7aa6c0122b90fa4c79c797efb158; BIGipServer~OEKB~POOL_my.oekb.at_kupl-services_rest_https=1293088143.13090.0000; BIGipServer~OEKB~POOL_kupl-kms.oekb.at_service_https=1293088143.13090.0000',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-GPC': '1',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
    }

    #send get requset for json to oekb server
    response = requests.get('https://my.oekb.at/fond-info/rest/public/steuerMeldung/isin/'+ i , headers=headers, cookies=cookies)
    j=response.json() #decode json
    #print(j)
    a=j['list'][0] #untangle json to one dict
    id=a['stmId'] #search doct for MeldeID
    print(id)


    #creat URL for csv of isin with MeldeID
    url="https://my.oekb.at/kms-reporting/public?report=steuerdaten-detail&fnameReplacement="+str(id)+"&MELDE_ID="+str(id)+"&BASIS&KENNZ_PRIVAT&ERTRS_BEH&DIVID&ZINS&ZINS_ALT&AUS_SUB&MLD_ERTR&MLD_DIVID&MLD_ZINS&MLD_ZINS_ALT&MLD_AUS_SUB&format=CSV"

    #download csv and save in directory
    savepath=atm+"\\"+i+".csv"
    urllib.request.urlretrieve(url, savepath)

    df = pandas.read_csv(savepath,on_bad_lines='skip',sep=';',names=['POSITION', 'BEZEICHNUNG', 'PA_MIT_OPTION', 'PA_OHNE_OPTION', 'BV_MIT_OPTION','BV_OHNE_OPTION','BV_JUR_PERSON','STIFTUNG','STEUERNAME','STEUERCODE'])

    df = df.drop(columns=['PA_MIT_OPTION','BEZEICHNUNG','PA_OHNE_OPTION', 'BV_MIT_OPTION','BV_OHNE_OPTION','BV_JUR_PERSON','STEUERNAME','STEUERCODE'])
    start = df.index[df['POSITION']=='1.'].to_list()[0]
    end = df.index[df['POSITION']=='16.4.'].to_list()[0]
    df = df.drop(list(range(0,start)))
    df = df.drop(list(range(end+1,df.last_valid_index()+1)))

    if first:
        masterdf = pandas.DataFrame(index=df['POSITION'].to_list(),data=df['STIFTUNG'].to_list(),columns=['AT0000A2HRU3'])
        first=False
    else:
        transferdf = pandas.DataFrame(index=df['POSITION'].to_list(),data=df['STIFTUNG'].to_list(),columns=['AT0000A2KVS3'])
        masterdf = masterdf.merge(transferdf, how='outer',left_index=True,right_index=True)

masterdf.to_excel("OeKB-Abruf vom "+atm+".xlsx","Stiftungen")



#curl "https://my.oekb.at/fond-info/rest/public/steuerMeldung/isin/AT0000A2KVS3" -H "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) Gecko/20100101 Firefox/98.0" -H "Accept: application/json" -H "Accept-Language: de" -H "Accept-Encoding: gzip, deflate, br" -H "OeKB-Platform-Context: eyJsYW5ndWFnZSI6ImRlIiwicGxhdGZvcm0iOiJLTVMiLCJkYXNoYm9hcmQiOiJLTVNfT1VUUFVUIn0=" -H "DNT: 1" -H "Connection: keep-alive" -H "Referer: https://my.oekb.at/kapitalmarkt-services/kms-output/fonds-info/sd/af/f?isin=AT0000A2KVS3" -H "Cookie: BIGipServer~OEKB~POOL_my.oekb.at_kapitalmarkt-services_https=1326642575.5410.0000; TS01deb77b=010780ef504b7f483da860f87289d76c2946a11e45e43dbf016b8254ebf5b6476d56ae19afbc9b7aa6c0122b90fa4c79c797efb158; BIGipServer~OEKB~POOL_my.oekb.at_kupl-services_rest_https=1293088143.13090.0000; BIGipServer~OEKB~POOL_kupl-kms.oekb.at_service_https=1293088143.13090.0000" -H "Sec-Fetch-Dest: empty" -H "Sec-Fetch-Mode: cors" -H "Sec-Fetch-Site: same-origin" -H "Sec-GPC: 1" -H "Pragma: no-cache" -H "Cache-Control: no-cache"