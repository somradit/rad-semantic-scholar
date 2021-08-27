import requests
import time
from datetime import datetime, timedelta
import pandas as pd

authors_papers_csv = "C:/Users/zeagle/scratch/Research/authors_papers.csv"
paper_details_csv = "C:/Users/zeagle/scratch/Research/paper_details_all.csv"

requestTiming = pd.DataFrame(columns=['request','timestamp'])
rateLimit = 80
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

def update_author_list():
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36'}
    publicWebsite = "https://rad.washington.edu/wp-json/people/v1/all/"
    publicWebsiteData = requests.get(publicWebsite, headers=headers,verify=False).json()

    authors = []

    for person in publicWebsiteData:
        if person['semantic_scholar_author_id']:
            authors.append(person['semantic_scholar_author_id'])
    return authors

def open_existing_paper_details(paper_details_csv):
    try:
        existing_papers_list = pd.read_csv(paper_details_csv)
    except FileNotFoundError:
        existing_papers_list = pd.DataFrame()
    return existing_papers_list

# Existing Author-Paper List
def open_existing_authorPaper_list(authorPaper_list_csv):
    try:
        existing_author_papers = pd.read_csv(authorPaper_list_csv, engine="python")
    except FileNotFoundError:
        existing_author_papers = pd.DataFrame(columns=['authorId','paperId'], dtype='str')
    else:
        print(existing_author_papers.info())
    return existing_author_papers

def open_exisiting_file(file):
    try:
        exisiting_list = pd.read_csv(file)
    except FileNotFoundError:
        exisiting_list = pd.DataFrame()
    return exisiting_list


def save_file(new_item, existing_list, file):
    paper_list_to_save = existing_list.append(new_item, sort=False, ignore_index=True)
    # paper_list_to_save = paper_list_to_save.drop_duplicates(ignore_index=True)
    paper_list_to_save.to_csv(file, index=False)

def get_author_papers(author): 
    global requestTiming

    while checkRate() > rateLimit:
        print("Sleeping for rate limit. Time remaining:" + str(timeRemaining()), end='\r')
        time.sleep(1)

    exisiting_author_papers = open_existing_authorPaper_list(authors_papers_csv)
    print(author)
    # Author Data API URL
    authorUrl = "https://api.semanticscholar.org/v1/author/"+author
    
    now = datetime.now()
    requestTiming = requestTiming.append({
        "request": authorUrl,
        "timestamp": pd.Timestamp(now.strftime("%m/%d/%Y %H:%M:%S"))
        }, ignore_index=True)
    print(requestTiming.tail(1))

    author_papers = pd.DataFrame(columns=['authorId','paperId'])
    authorDetails = requests.get(authorUrl).json()
    papers = authorDetails['papers']
    for paper in papers:
        author_papers.loc[-1] = [authorDetails['authorId'],paper['paperId']]
        author_papers.index = author_papers.index + 1  # shifting index
        author_papers = author_papers.sort_index()  # sorting by index
    print(author_papers)
    save_paper_list(author_papers, exisiting_author_papers)
    return author_papers
    
def get_paper_details(author_papers):
    global requestTiming

    paper_details = pd.DataFrame(dtype='object')
    existing_paper_details = open_existing_paper_details(paper_details_csv)
    for index, row in author_papers.iterrows():
        try: 
            paperId = str(row['paperId'])
        except:
            paperId = None

        try:
            exisiting_ids = existing_paper_details.paperId.values
        except:
            exisiting_ids = []

        if paperId in exisiting_ids:
            print('exists')
        else:
            print('doesnt exist')
            while checkRate() > rateLimit:
                print("Sleeping for rate limit. Time remaining:" + str(timeRemaining()), end='\r')
                time.sleep(1)
            paperURL = "https://api.semanticscholar.org/v1/paper/"+row['paperId']

            now = datetime.now()
            requestTiming = requestTiming.append({
                "request": paperURL,
                "timestamp": pd.Timestamp(now.strftime("%m/%d/%Y %H:%M:%S"))
                }, ignore_index=True)
            print(requestTiming.tail(1))

            paperDetails = requests.get(paperURL).json()
            print(paperDetails['title'])
            paper_details = paperDetails
            # paper_details.index = paper_details.index + 1  # shifting index
            # paper_details = paper_details.sort_index()  # sorting by index
            exisiting_papers = open_exisiting_file(paper_details_csv)
            save_file(paper_details, exisiting_papers, paper_details_csv)
            # paper_details_to_save = existing_paper_details.append(paper_details, sort=False)
            # paper_details_to_save = paper_details_to_save.drop_duplicates(ignore_index=True)
            # paper_details_to_save.to_csv(paper_details_csv, index=False)


def save_paper_list(author_papers, exisiting_paper_list):
    paper_list_to_save = exisiting_paper_list.append(author_papers, sort=False)
    paper_list_to_save = paper_list_to_save.drop_duplicates(ignore_index=True)
    paper_list_to_save.to_csv(authors_papers_csv, index=False)


def main():
    authors = update_author_list()
    

    for author in authors:
        author_papers = get_author_papers(author)
        
        if not author_papers.empty:
            get_paper_details(author_papers)
            #for paper in author_papers:
            #   save_author_papers(author_papers, papers_list, exisiting_authorPapers, existing_papers)

if __name__ == "__main__":
    main()