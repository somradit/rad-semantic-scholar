import pandas as pd

papers_csv = 'C:/Users/zeagle/scratch/Research/paper_details_all.csv'

citation_info = 'C:/Users/zeagle/scratch/Research/ss_citation_info.csv'
paper_info = 'C:/Users/zeagle/scratch/Research/ss_paper_info.csv'
topic_info = 'C:/Users/zeagle/scratch/Research/ss_topic_info.csv'
author_info = 'C:/Users/zeagle/scratch/Research/ss_author_info.csv'
fieldsOfStudy_info = 'C:/Users/zeagle/scratch/Research/ss_fieldsofstudy_info.csv'

papers = pd.read_csv(papers_csv)

citations = papers[['paperId','citations','references']]
citations.to_csv(citation_info, index=False)

topics = papers[['paperId','topics']]
topics.to_csv(topic_info, index=False)

authors = papers[['paperId','authors']]
authors.to_csv(author_info, index=False)

fields = papers[['paperId','fieldsOfStudy']]
fields.to_csv(fieldsOfStudy_info, index=False)


papers_without_citations = papers.drop(columns=['citations','references','topics','authors','fieldsOfStudy'])
papers_without_citations.to_csv(paper_info, index=False)
