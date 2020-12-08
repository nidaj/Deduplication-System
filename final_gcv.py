import pandas as pd
import pandas_dedupe
import numpy as np
print('Importing of libraries done')
data_gcv_cano = pd.read_csv('Client_data.csv')
for j in data_gcv_cano.columns:
     for i in data_gcv_cano.index:
        if((data_gcv_cano[j][i]==0)|(data_gcv_cano[j][i]=='0')|(data_gcv_cano[j][i]=='')|(data_gcv_cano[j][i]==np.NaN)):
            data_gcv_cano[j][i]="None"    
data_gcv_cano.replace('', "None",inplace=True)
data_gcv_cano.replace(' ', "None",inplace=True)
data_gcv_cano.replace('0', "None",inplace=True)
data_gcv_cano.replace(np.NaN, "None",inplace=True)
data_gcv_cano['Doctor Name'] = data_gcv_cano['Doctor Name'].str.lower()
data_gcv_cano['Doctor Name'] = data_gcv_cano['Doctor Name'].str.replace('dr |dr. |mrs|mrs.|mr|mr.|ms|ms.|\(|\)|', '')
data_gcv_cano['Doctor Name'] = data_gcv_cano['Doctor Name'].str.replace('\. ',' ')
data_gcv_cano['Doctor Name'] = data_gcv_cano['Doctor Name'].str.replace('\.',' ')
print(data_gcv_cano.head(5))
data_gcv_cano = pandas_dedupe.dedupe_dataframe(data_gcv_cano,['Doctor PKID','Doctor Code','Doctor Name','Email','Tel Clinic','Tel Res','Registration No','Address','Street','Area Name','City Name','State','District','Taluka','City','Pincode','Specialization','Territory Name'],canonicalize=True)
print("Reached")
data_gcv_cano.sort_values('cluster id',ascending=True)
#data_gcv_cano = pd.read_csv('gcv_cano.csv',index_col=[0])
gcv_cano_clustered = data_gcv_cano[data_gcv_cano['cluster id']>=0]
gcv_cano_non_clustered = data_gcv_cano[data_gcv_cano['cluster id'].isnull()]
cols_org2 = [c for c in gcv_cano_clustered.columns if ((c.lower()[:10] != 'canonical_') & ( c.lower()[:10] != 'cluster id') & ( c.lower()[:10] != 'confidence'))]
cols_cano2 = [c for c in gcv_cano_clustered.columns if ((c.lower()[:10] == 'canonical_') & ( c.lower()[:10] != 'cluster id') & ( c.lower()[:10] != 'confidence'))]
data_gcv_cano_2=data_gcv_cano.drop(columns=cols_cano2)
data_gcv_cano_2.to_csv('Final_Output1.csv')
max_clustered_id = int(gcv_cano_clustered['cluster id'].max())
l=[]
                    #for c_id in range(0,max_clustered_id+1)
l=gcv_cano_clustered.isnull().sum(axis=1)
for i in cols_org2:
      gcv_cano_clustered[i].fillna(gcv_cano_clustered['canonical_'+i], inplace=True)
for i in gcv_cano_clustered.index:
      if ((gcv_cano_clustered['canonical_Specialization'][i]!='gp') & (gcv_cano_clustered['canonical_Specialization'][i]!='general practitioner')&(gcv_cano_clustered['Specialization'][i]!=gcv_cano_clustered['canonical_Specialization'][i])&(pd.notna(gcv_cano_clustered['canonical_Specialization'][i])==True)):
        gcv_cano_clustered['Specialization'][i]=str(gcv_cano_clustered['Specialization'][i]) + ',' + str(gcv_cano_clustered['canonical_Specialization'][i])
gcv_cano_clustered['null_score']=l
unique_dup=gcv_cano_clustered.sort_values(by='cluster id')
unique_dup.drop(columns=cols_cano2,inplace=True)
l2=[]
for c_id in range(0,max_clustered_id+1):
    val = (unique_dup['cluster id']==c_id).idxmax()
    l2.append(val)
unique_dup.loc[l2,'Unique/Duplicate'] = 'Unique'
unique_dup['Unique/Duplicate'].fillna('Duplicate',inplace=True)
unique_dup.drop(columns='null_score',inplace=True)

gcv_cano_non_clustered['Unique/Duplicate']='Unique'
gcv_cano_non_clustered.drop(columns=cols_cano2,inplace=True)
final_csv = pd.concat([unique_dup,gcv_cano_non_clustered],axis=0)
final_csv.to_csv('Final_output2.csv')
unique_csv = pd.read_csv('Final_output2.csv')
filter = unique_csv["Unique/Duplicate"]=="Unique"
unique_csv.where(filter, inplace = True)
unique_csv.dropna(axis=0, how='all', inplace = True)
unique_csv.to_csv("Final_output3.csv")
print('Output generated')
