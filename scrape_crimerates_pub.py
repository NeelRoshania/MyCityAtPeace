# Commands to run script on cloud,
#     - source venv/bin/activate -> activate the virtual environment
#     - chmod +x myscript.py -> To make the script executable
#     - nohup python3 -u scrape_crimerates.py > ../output/output.log & -> To make sure the process runs after logout
#     - logout -> to logout
#     - tail output.log -> to check output logs
#     - ps -e | grep myscript.py -> to check status

#     https://stackoverflow.com/questions/47455680/running-a-python-script-on-google-cloud-compute-engine
#     https://janakiev.com/til/python-background/


import pandas as pd
import numpy as np
import re
import requests
from bs4 import BeautifulSoup

# import spreadsheet, drop nans
print("\nProgram start\n")
print("\t- Importing base data", end='\r')
dfo = pd.read_excel("crimerates.xlsx", sheet_name='analysis', dtype={'zipcode': 'str', 
                                                                   'oztract': 'str'}).dropna(axis=0).reset_index(drop=True)
print("\t- Importing base data...complete!")

# Initializations
print("\t- Initializing columns and variables", end='\r')
dfo["town"] = ""
dfo["state_abbr"] = ""
dfo["violentCrime"] = ""
dfo["propertyCrime"] = ""
dfo["p_unemploymentRate"] = ""
dfo["p_costOfLivingToUS"] = ""
dfo["population"] = ""
dfo["p_populationGrowth"] = ""
dfo["median_REval"] = ""
dfo["p_medianREGrowth"] = ""
dfo["aveus_violentCrime"] = ""
dfo["aveus_propertyCrime"] = ""
dfo["aveus_punemploymentRate"] = ""
dfo["status_base"] = ""
dfo["status_more"] = ""
dfo["url_base"] = ""
dfo["url_more"] = ""

base = "https://www.bestplaces.net/crime/zip-code" # state/country/zipcode
moredata = "https://www.bestplaces.net/zip-code"
notFoundbase = []
notFoundmore = []
errorurl = []
fileout = "out/ozcrime_stats_prod.csv"
df = dfo.copy()

# iterange = range(len(df.index)-2, len(df.index))
# iterange = range(500, 496)
iterange = range(0, len(df.index))

# Scraping Process
    # # Check NaN's in each column
    # df.isna().sum()

    # Iterate between each row index
    #     
    #     - build base url
    #       - extract state, country and zip
    #     - build moredata url
    #     - request to base and more data
    #         - push both html to bs4
    #         - extract violent crime data, additional
    #         - extract additional demographic data
print("\t- Initializing columns and variables...complete!")
print("\t- Scraping in progress...\n")
for i in iterange:
    baseurl = "{}/{}/{}/{}".format(base, df.loc[i, 'state'].lower(), df.loc[i, 'country'].lower(), df.loc[i, 'zipcode'])
    moredataurl = "{}/{}/{}/{}".format(moredata, df.loc[i, 'state'].lower(), df.loc[i, 'country'].lower(), df.loc[i, 'zipcode'])

    print("\t\tProgress: {} of {} urls.\t{} links with errors\t{} links with no crime\t{} links with no additional data".format(i+1, len(df.index), len(errorurl), len(notFoundbase), len(notFoundmore)), end="\r")
    # print('Current url: {}\n'.format(moredataurl))
    
    df.loc[i, "url_base"] = baseurl
    df.loc[i, "url_more"] = moredataurl
    
    request_base = requests.get(baseurl)
    request_more = requests.get(moredataurl)
    df.loc[i, "status_base"] = request_base.status_code
    df.loc[i, "status_more"] = request_more.status_code
    
#     scrape base data
    try:
        if (request_base.status_code == 200):
            soup = BeautifulSoup(request_base.content, features="html.parser")
            #  if zip is found
            if re.search(r'Not Found', soup.title.text) is None:
    #             title = re.search(r'\((\w+\s*\w*), (\w+)\)|\((\w+), (\w+)\)', soup.title.text)
                title = re.search(r'\(([a-zA-Z0-9_.,\- ]+), (\w+)\)|\((\w+), (\w+)\)', soup.title.text)
                match_v = re.search(r'violent crime is\s+(?P<vcrime>\w+\.\w+)', str(soup)) 
                match_p = re.search(r'property crime is\s+(?P<pcrime>\w+\.\w+)', str(soup)) 
                average = re.findall(r'\(The US average is\s+(?P<average>\w+.\w)\)', str(soup))

                #  if matches are not empty
                if title:
                    df.loc[i, "town"] = title.group(1)
                    df.loc[i, "state_abbr"] = title.group(2)
                else:
                    df.loc[i, "town"] = None   
                if match_v:
                    df.loc[i, "violentCrime"] = match_v.group("vcrime")
                if match_p:
                    df.loc[i, "propertyCrime"] = match_p.group("pcrime")
                if average:
                    df.loc[i, "aveus_violentCrime"] = average[0]
                    df.loc[i, "aveus_propertyCrime"] = average[1]
                # else:
                #     df.loc[i, "violentCrime"] = None
                #     df.loc[i, "propertyCrime"] = None
                #     df.loc[i, "aveus_violentCrime"] = None
                #     df.loc[i, "aveus_propertyCrime"] = None
            else:
                notFoundbase.append(baseurl)
                
    #   scrape more data
        if (request_more.status_code == 200):
            soup_more = BeautifulSoup(request_more.content, features="html.parser")
            
            #  if zip is found
            if re.search(r'Not Found', soup_more.title.text) is None:
                unempl_str = "The unemployment rate in {} (zip {}) is".format(title.group(1), df.loc[i, 'zipcode'])
                avgunempl_str = "(U.S. avg. is"
                col_str = "cost of living is"
                pop_str = ["population is",  "Since 2010, it has had a population", "of"]
                restate_str = ["The median home cost in {} (zip {}) is ".format(title.group(1), df.loc[i, 'zipcode']), "Home", "the last 10 years has been"]
                
                unempl = re.search(r'' + re.escape(unempl_str) 
                            + r'\s+(\w+\.\w+)\%\s+' + re.escape(avgunempl_str) 
                            + r'\s+(\w+.\w+)\%\)', str(soup_more))
                col = re.search(r'' + re.escape(col_str) 
                                + r'\s+(\w+\.\w+)\%\s(\w+)', str(soup_more))
                pop = re.search(r'' + re.escape(pop_str[0]) 
                                + r'\s+((\d{0,3},)?(\d{3},)?\d{0,3})\s+people\.\s+' + re.escape(pop_str[1])
                                + r'\s+(\w+)\s+' + re.escape(pop_str[2])
                                + r'\s+(\w+.\w*)\%', str(soup_more))
                restate = re.search(r'' + re.escape(restate_str[0]) 
                                + r'\$((\d{0,3},)?(\d{3},)?\d{0,3}).\s+' + restate_str[1]
                                + r'\s+(\w+)\s+' + restate_str[2] 
                                + r'\s+([-]*\w+.\w+)\%', str(soup_more))
                
                if unempl:
                    df.loc[i, 'p_unemploymentRate'] = float(unempl.group(1))/100
                    df.loc[i, 'aveus_punemploymentRate'] = float(unempl.group(2))/100
                if col:
                    if col.group(2) == "lower":
    #                     print(-float(col.group(1)))
                        df.loc[i, 'p_costOfLivingToUS'] = -float(col.group(1))/100
                    else:
                        df.loc[i, 'p_costOfLivingToUS'] = float(col.group(1))/100
                if pop:
                    df.loc[i, 'population'] = float(pop.group(1).replace(",", ""))
                    if pop.group(4) == "decline":
    #                     print(-float(pop.group(3).replace(",", "")))
                        df.loc[i, 'p_populationGrowth'] = -float(pop.group(5))/100
                    else:
                        df.loc[i, 'p_populationGrowth'] = float(pop.group(5))/100
                if restate:
                    df.loc[i, 'median_REval'] = float(restate.group(1).replace(",", ""))
                    df.loc[i, 'p_medianREGrowth'] = float(restate.group(5))/100
            else:
                notFoundmore.append(moredataurl)
    except:
        errorurl.append((baseurl, moredataurl))

# convert columns to retain leading zeros
df.zipcode = df.zipcode.apply('="{}"'.format)
df.oztract = df.oztract.apply('="{}"'.format)

print("\n\n\t-Scraping complete\n\t-Starting export to {}".format(fileout))
df.to_csv(fileout)
print("\t-Export done!\n")

if notFoundbase:
    print("\t-Crime data not found on {} links: \n\t{}\n".format(len(notFoundbase), notFoundbase))
    pd.DataFrame(data=notFoundbase, columns=["url"]).to_csv("out/noCrimeData.csv")
if notFoundmore:
    print("\t-Demographic info not found on {} links: \n\t{}\n".format(len(notFoundmore), notFoundmore))
    pd.DataFrame(data=notFoundmore, columns=["url"]).to_csv("out/noDemographicData.csv")
if errorurl:
    print("\t-Links sets that generated errors - {} links: \n\t{}\n".format(len(errorurl), errorurl))
    pd.DataFrame(data=errorurl, columns=["base", "moredata"]).to_csv("out/errorurls.csv")