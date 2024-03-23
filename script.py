import pandas as pd
import pysolr as ps

# Read the csv file
df = pd.read_csv('aluno.csv')

# Drop the NA rows
df.dropna(inplace=True)

# Convert Data de Nascimento into a correct Format
df['Data de Nascimento'] = pd.to_datetime(df['Data de Nascimento'])
df.dropna(subset=['Data de Nascimento'], inplace = True)

# Ensure consistency for Série and Nota Média
for x in df.index:
    if df.loc[x, "Nota Média"] > 10 or df.loc[x, "Nota Média"] < 0:
        df.drop(x, inplace = True)
    if df.loc[x, "Série"] > 7 or df.loc[x, "Série"] <= 1:
        df.drop(x, inplace = True)


solr = ps.Solr('http://localhost:8983/solr/alunos', always_commit=True)

# Do a health check.
try:
    solr.ping()
except:
    print("Something went wrong with solr instance, check if the path is correct")
else:
    # Convert the DataFrame into a Dict List
    df['Data de Nascimento'] = df['Data de Nascimento'].astype(str)
    dados = df.to_dict('records')

    # Add the data into solr
    solr.add(dados)

    # Transfer confirmation
    solr.commit()

    # Search
    results = solr.search('*:*')
    for result in results:
        print(result)