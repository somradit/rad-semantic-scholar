import pandas as pd
import ftfy 

broken = 'C:/Users/zeagle/scratch/Research/paper_details_doi.csv'
fixed = 'C:/Users/zeagle/scratch/Research/paper_details_fixed.csv'

broken_df = pd.read_csv(broken).dropna(subset=['title'])
broken_df_na = pd.read_csv(broken)
combined_list = pd.merge(broken_df, broken_df_na, how='outer', indicator="Exists", left_on='paperId',right_on='paperId')
print(combined_list)
print(combined_list[combined_list['Exists']!='both'])
print(broken_df['title'])
broken_df['title'] = [ftfy.fix_text(x) for x in broken_df['title']]

broken_df.to_csv(fixed, index=False)