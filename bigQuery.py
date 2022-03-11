import pandas as pd
from IPython.core.display import display
from google.oauth2 import service_account


credentials = service_account.Credentials.from_service_account_file(
    '/home/andrey/Projects/d#######.json')

query = '''
SELECT DATE(creation_date) as date, COUNT(id) as questions
FROM
  `bigquery-public-data.stackoverflow.posts_questions`
WHERE tags LIKE '%pandas%'
GROUP BY
  date
'''

project_id = '#######'

df = pd.read_gbq(query, project_id=project_id, credentials=credentials)


df.head(5)

display(df.head(5))

df['month'] =  df['date'].values.astype('datetime64[M]')
df['year'] =  df['date'].values.astype('datetime64[Y]')
# display(df.sort_values('questions', ascending=False).head(1))


stats = df.groupby(['year','month'],as_index=False).agg({'questions':'sum'})
display(stats.sort_values('questions',ascending=False).head(5))

year_stats = stats[(stats.month >= '2013-01-01') & (stats.month < '2018-09-01')].groupby(['year'],as_index=False).agg({'questions':['mean','sum']})

display(year_stats)
year_stats['estimate'] = year_stats[('questions','mean')]*12
year_stats.columns = ['year','mean_questions','sum_questions','estimate']

year_stats.to_gbq('cl_test.Clients', project_id=project_id, if_exists='fail')