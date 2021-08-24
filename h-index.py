import numpy as np
from numpy.core.defchararray import index
import pandas as pd

citations_csv = "C:/Users/zeagle/scratch/Research/citations_2021-08-19.csv"
papers_csv  = "C:/Users/zeagle/scratch/Research/authors_papers.csv"

citations = pd.read_csv(citations_csv)
papers = pd.read_csv(papers_csv)

papers_and_citations = pd.merge(papers,citations,'inner',left_on="paperId",right_on="paperId")
print(papers_and_citations)
authors = papers['authorId'].unique()

h_index_frame = pd.DataFrame(columns=['authorId','h_index'])

for author in authors:
    author_papers = papers_and_citations[papers_and_citations['authorId']==author]

    #https://towardsdatascience.com/fastest-way-to-calculate-h-index-of-publications-6fd52e381fee
    citations = np.array(author_papers['citations'])
    n = citations.shape[0]
    array = np.arange(1,n+1)

    citations = np.sort(citations)[::-1]


    h_index = np.max(np.minimum(citations, array))
    h_index_frame = h_index_frame.append({'authorId':author,'h_index':h_index}, ignore_index=True)

h_index_frame.to_csv("C:/Users/zeagle/scratch/Research/h_indexes.csv", index=False)
