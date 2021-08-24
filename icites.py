from pandas.core.arrays.sparse import dtype
from pandas.core.dtypes import dtypes
import requests
import pandas as pd

paper_list_csv = 'C:/Users/zeagle/scratch/Research/external_ids.csv'
icite_list_csv = 'C:/Users/zeagle/scratch/Research/icites.csv'

def open_exisiting_file(file, columns):
    try:
        exisiting_list = pd.read_csv(file, dtype=object)
    except FileNotFoundError:
        exisiting_list = pd.DataFrame(columns=columns, dtype=object)
    return exisiting_list


def save_file(new_item, existing_list, file):
    paper_list_to_save = existing_list.append(new_item, sort=False, ignore_index=True)
    paper_list_to_save = paper_list_to_save.drop_duplicates(subset=['pmid'], ignore_index=True)
    paper_list_to_save.pmid = paper_list_to_save.pmid.astype(str)
    print(paper_list_to_save.pmid)
    paper_list_to_save.to_csv(file, index=False)


#get list of papers from semantic scholar list
paper_list = pd.read_csv(paper_list_csv, dtype=str)
paper_list = paper_list[['PubMed',]]
paper_list = paper_list.dropna()
#get existing list of cross ref 
icites = open_exisiting_file(icite_list_csv, ['pmid'])
icites = icites['pmid']

combined_list = pd.merge(paper_list, icites, how='outer', indicator="Exists", left_on='PubMed',right_on='pmid')
missing_details = combined_list[combined_list["Exists"] == 'left_only']

pmids = missing_details['PubMed']

i = 0
n = pmids.shape[0]
while i < n:
    query = pmids[i:i+100].str.cat(sep=',')
    print(query)

    request = 'https://icite.od.nih.gov/api/pubs?pmids='+query
    icites = requests.get(request).json()
    new_icites = pd.json_normalize(icites['data'])
    new_icites['pmid'] = new_icites['pmid'].astype(str)
    print(new_icites)
    existing_icites = open_exisiting_file(icite_list_csv, ['PubMed'])
    save_file(new_icites, existing_icites, icite_list_csv)

    # for pmid in pmids['records']:
    #     if 'pmid' in pmid:
    #         pmid_list = pmid_list.append({'doi':pmid['doi'],'pmid':pmid['pmid']}, ignore_index=True)

    i = i+100

# pmid_list.to_csv('C:/Users/zeagle/scratch/Research/pmids.csv', index=False)