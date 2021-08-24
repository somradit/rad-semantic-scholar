from pandas._libs import missing
import requests
import time
from datetime import datetime, timedelta
import pandas as pd
pd.options.display.show_dimensions = False

crossref_paper_csv = "C:/Users/zeagle/scratch/Research/crossref_paper_data.csv"
paper_details_csv = "C:/Users/zeagle/scratch/Research/paper_details_doi.csv"
crossref_citations_folder = "C:/Users/zeagle/scratch/Research/"
#crossref_citations_csv = crossref_citations_folder + "citations_"+ datetime.now().strftime("%Y-%m-%d"+".csv")
crossref_citations_csv = crossref_citations_folder + "citations_2021-08-19.csv"

headers = {'User-Agent': 'UWRadiology/0.1 (https://rad.washington.edu; mailto:somradit@uw.edu)'}

paper_details = pd.DataFrame(columns=['paperId','doi','title','createdYear','createdMonth','createdDay','printYear','printMonth','onlineYear','onlineMonth','type', 'container', ])
citations_dataframe = pd.DataFrame(columns=['paperId','doi', 'citations'])
requestTiming = pd.DataFrame(columns=['request','timestamp'])
rateLimit = 1000
#rate limit time in minutes
rateTime = 1

def checkRate():
    now = datetime.now()
    rateLimitBeginning = pd.Timestamp(now - timedelta(hours=0, minutes=rateTime,seconds=0))
    filtermask = requestTiming['timestamp'] > rateLimitBeginning
    filteredDf = requestTiming[filtermask]
    return filteredDf.shape[0]


def timeRemaining():
    now = datetime.now()
    rateLimitBeginning = pd.Timestamp(now - timedelta(hours=0, minutes=rateTime,seconds=0))
    filtermask = requestTiming['timestamp'] > rateLimitBeginning
    filteredDf = requestTiming[filtermask]
    sortedDf = filteredDf.sort_values(by='timestamp', ascending=True, na_position='last')
    timeleft = (now - sortedDf.iloc[1]['timestamp']).seconds
    timeleft = rateTime*60 - timeleft
    return timeleft

def open_existing_crossref_details():
    try:
        existing_papers_list = pd.read_csv(crossref_paper_csv, dtype="str")
    except FileNotFoundError:
        existing_papers_list = pd.DataFrame(columns=['paperId','doi','title','createdYear','createdMonth','createdDay','printYear','printMonth','onlineYear','onlineMonth','type', 'container', ], dtype='str')
    return existing_papers_list

def open_existing_crossref_citations():
    try:
        exisiting_citations_list = pd.read_csv(crossref_citations_csv, dtype="str")
    except FileNotFoundError:
        exisiting_citations_list = pd.DataFrame(columns=['paperId','doi','citations', ], dtype='str')
    return exisiting_citations_list

def open_existing_paper_details():
    try:
        existing_papers_list = pd.read_csv(paper_details_csv, dtype="str")
    except FileNotFoundError:
        existing_papers_list = pd.DataFrame(columns=['paperId','title','year','venue','numCitedBy','url','doi'], dtype='str')
    return existing_papers_list

def get_paper_details(paperId, doi):
    global requestTiming

    while checkRate() > rateLimit:
        print("Sleeping for rate limit. Time remaining:" + str(timeRemaining()), end='\r')
        time.sleep(1)

    existing_crossref_details = open_existing_crossref_details()
    request = "https://api.crossref.org/works/"+str(doi)

    now = datetime.now()
    requestTiming = requestTiming.append({
        "request": request,
        "timestamp": pd.Timestamp(now.strftime("%m/%d/%Y %H:%M:%S"))
        }, ignore_index=True)

    requestData = requests.get(request, headers=headers)
    if requestData.status_code == 200:
        requestDataJson = requestData.json()
        message = requestDataJson["message"]

        try: createdYear = message["created"]["date-parts"][0][0]
        except (KeyError,IndexError): createdYear = 'null'
        try: createdMonth = message["created"]["date-parts"][0][1]
        except (KeyError,IndexError): createdMonth = 'null'
        try: createdDay = message["created"]["date-parts"][0][2]
        except (KeyError,IndexError): createdDay = 'null'

        try: printYear = message["published-print"]["date-parts"][0][0]
        except (KeyError,IndexError): printYear = 'null'
        try: printMonth = message["published-print"]["date-parts"][0][1]
        except (KeyError,IndexError): printMonth = 'null'
        try: onlineYear = message["published-online"]["date-parts"][0][0]
        except (KeyError,IndexError): onlineYear = 'null'
        try:onlineMonth = message["published-online"]["date-parts"][0][1]
        except (KeyError,IndexError): onlineMonth = 'null'

        try:contentType = message["type"]
        except (KeyError,IndexError): contentType = 'null'
        try: container = message["container-title"][0]
        except (KeyError,IndexError): container = 'null'
        try: title = message["title"][0]
        except (KeyError,IndexError): title = 'null'

        paper_details.loc[-1] = [
                                paperId,
                                doi,
                                title,
                                createdYear,
                                createdMonth,
                                createdDay,
                                printYear,
                                printMonth,
                                onlineYear,
                                onlineMonth,
                                contentType,
                                container
        ]
        print(paper_details.to_string(header=False))
        paper_details_to_save = existing_crossref_details.append(paper_details, sort=False)
        paper_details_to_save = paper_details_to_save.drop_duplicates(ignore_index=True)
        paper_details_to_save.to_csv(crossref_paper_csv, index=False)
    else:
        print(requestData.status_code)
        print("http error")

def get_citations(paperId, doi):
    global requestTiming
    existing_crossref_citations = open_existing_crossref_citations()
    while checkRate() > rateLimit:
        print("Sleeping for rate limit. Time remaining:" + str(timeRemaining()), end='\r')
        time.sleep(1)

    request = "https://api.crossref.org/works/"+str(doi)

    now = datetime.now()
    requestTiming = requestTiming.append({
        "request": request,
        "timestamp": pd.Timestamp(now.strftime("%m/%d/%Y %H:%M:%S"))
        }, ignore_index=True)

    requestData = requests.get(request, headers=headers)
    if requestData.status_code == 200:
        requestDataJson = requestData.json()
        message = requestDataJson["message"]
        try: citations = message["is-referenced-by-count"]
        except (KeyError,IndexError): citations = 'null'
        citations_dataframe.loc[-1] = [
                                paperId,
                                doi,
                                citations
        ]
        print(str(checkRate()) + ": " + citations_dataframe.to_string(header=False))
        citations_to_save = existing_crossref_citations.append(citations_dataframe, sort=False)
        citations_to_save = citations_to_save.drop_duplicates(ignore_index=True)
        citations_to_save.to_csv(crossref_citations_csv, index=False)
    else:
        print(requestData.status_code)
        print("http error")


def main():
    #get list of papers from semantic scholar list
    existing_paper_details = open_existing_paper_details()
    existing_paper_details = existing_paper_details[['paperId','doi']]
    existing_paper_details = existing_paper_details.dropna()
    #get existing list of cross ref 
    existing_crossref_details = open_existing_crossref_details()
    existing_crossref_details = existing_crossref_details[['paperId','doi']]
    #compare the two lists and remove papers we already have crossref info for
    combined_list = pd.merge(existing_paper_details, existing_crossref_details, how='outer', indicator="Exists")
    missing_details = combined_list[combined_list["Exists"] == 'left_only']
    for index, row in missing_details.iterrows():
        get_paper_details(row['paperId'],row['doi'])

    #for index, row in existing_paper_details.iterrows():
        #get_citations(row['paperId'],row['doi'])

if __name__ == "__main__":
    main()