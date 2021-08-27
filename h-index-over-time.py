import pandas as pd
import numpy as np
import json
from ast import literal_eval

citation_csv = 'C:/Users/zeagle/scratch/Research/ss_citation_info.csv'
papers_csv  = "C:/Users/zeagle/scratch/Research/authors_papers.csv"

citations = pd.read_csv(citation_csv,  converters={'citations': literal_eval})
papers = pd.read_csv(papers_csv)
authors = papers['authorId'].unique()

# authorId = 144119174
# papers = papers[papers['authorId']==authorId]
papers = papers.rename(columns={'paperId':'originalPaperId'})
papers_and_citations = pd.merge(papers,citations,'inner',left_on="originalPaperId",right_on="paperId")
papers_and_citations = papers_and_citations.explode('citations').reset_index()
papers_and_citations = pd.concat([papers_and_citations.drop(['citations'], axis=1), papers_and_citations['citations'].apply(pd.Series)], axis=1)
papers_and_citations['year'] = pd.to_numeric(papers_and_citations['year'])
h_index_frame = pd.DataFrame()

for author in authors:
    print(author)
    papers = papers_and_citations[papers_and_citations['authorId']==author]
    try:
        startYear = min(papers['year'].dropna())
    except:
        startYear = 2022
        print("bad: "+ str(author))
    endYear = 2021
    i = int(startYear)
    while i <= endYear:
        citations_by_year = papers[papers['year']<=i]
        citations_by_year = citations_by_year.groupby(['originalPaperId']).size().reset_index(name='citations')
        
        author_citations = np.array(citations_by_year['citations'])
        n = author_citations.shape[0]
        array = np.arange(1,n+1)

        author_citations = np.sort(author_citations)[::-1]


        h_index = np.max(np.minimum(author_citations, array))
        print(h_index)
        h_index_frame = h_index_frame.append({'authorId':author,'year':i,'h_index':h_index}, ignore_index=True)
        i = i+1
    print(h_index)
h_index_frame.to_csv('C:/Users/zeagle/scratch/Research/h_index_by_year.csv', index=False)