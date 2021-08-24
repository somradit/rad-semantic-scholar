import pandas as pd
from pandas._libs import missing
import requests
from datetime import datetime, timedelta
import time

authors_papers_csv = "C:/Users/zeagle/scratch/Research/authors_papers.csv"
external_id_csv = "C:/Users/zeagle/scratch/Research/external_ids.csv"
papers = pd.read_csv(authors_papers_csv)



requestTiming = pd.DataFrame(columns=['request','timestamp'])
rateLimit = 75
#rate limit time in minutes
rateTime = 5

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

def open_exisiting_file(file):
    try:
        exisiting_list = pd.read_csv(file, dtype="str")
    except FileNotFoundError:
        exisiting_list = pd.DataFrame()
    return exisiting_list


def save_file(new_item, existing_list, file):
    paper_list_to_save = existing_list.append(new_item, sort=False, ignore_index=True)
    paper_list_to_save = paper_list_to_save.drop_duplicates(ignore_index=True)
    paper_list_to_save.to_csv(file, index=False)




existing_ids = open_exisiting_file(external_id_csv)

#get list of papers from semantic scholar list
paper_list = open_exisiting_file(authors_papers_csv)
paper_list = paper_list[['paperId',]]
paper_list = paper_list.dropna()
print(paper_list)
#get existing list of cross ref 
id_list = open_exisiting_file(external_id_csv)
id_list = id_list[['paperId',]]
print(id_list)

combined_list = pd.merge(paper_list, id_list, how='outer', indicator="Exists", left_on='paperId', right_on='paperId')
print(combined_list[combined_list['Exists'] != 'both'])
missing_details = combined_list[combined_list['Exists'] != 'both']
print(missing_details)
combined_list.to_csv('C:/Users/zeagle/scratch/Research/test.csv')
paperIds = missing_details['paperId']
print(paperIds)

externalIds_list = pd.DataFrame()
for paper in paperIds:
    request = 'https://api.semanticscholar.org/graph/v1/paper/'+paper+'?fields=externalIds'
    now = datetime.now()
    requestTiming = requestTiming.append({
        "request": request,
        "timestamp": pd.Timestamp(now.strftime("%m/%d/%Y %H:%M:%S"))
        }, ignore_index=True)
    while checkRate() > rateLimit:
        print("Sleeping for rate limit. Time remaining:" + str(timeRemaining()), end='\r')
        time.sleep(1)
    paperInfo = requests.get(request).json()
    externalIds = paperInfo['externalIds']
    externalIds["paperId"] = paper
    #externalIds_list = externalIds_list.append(externalIds, ignore_index=True)
    print(externalIds)
    existing_ids = open_exisiting_file(external_id_csv)
    save_file(externalIds, existing_ids, external_id_csv)
