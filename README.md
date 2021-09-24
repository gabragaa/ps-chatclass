# chatclass/data-case
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 17 13:54:26 2021

@author: gabri
"""
#dict = json no python
#lambda x é uma função rapida
#axis=1: orienta de cima para baixo
#axis=0 : orienta da esquerda pra direita
#para usar o normalize, precisamos passar ou uma list ou dict
#unnest: desembaraça lista (torna a plana)


#Importação das bibliotecas utilizadas
import pandas as pd
import numpy as np
from ast import literal_eval
from itertools import chain
import sqlite3
import json
import requests

#leitura das colunas 'movie_id' e 'title' do arquivo 'credits' em csv
local_file=r"C:\Users\gabri\Case Chat Class\data-case-v1-master\tmdb_5000_credits.csv"
movie=pd.read_csv(local_file,usecols=[0,1])

# leitura da coluna 'crew'  do arquivo 'credits' em csv
# transformando as strings em objeto usando literal_eval
credit=pd.read_csv(local_file,converters={'crew':literal_eval})


# função para adicionar o item 'title' no json
def update_lista(json_list,title):
    
    new_list=[]
    for i in json_list:
        new_dict=i
        new_dict.update({"title":title})
        new_list.append(new_dict)
    
    return new_list           
    

# coluna que adiciona coluna 'new_crew' atualizada com o título de cada filme no json
credit["new_crew"]=credit.apply(lambda linha: update_lista( linha.crew,linha.title), axis=1)

# transforma coluna json em lista
lista_crew=list(chain.from_iterable(credit.new_crew.to_list()))

# transforma lista em um DataFrame
df_crew=pd.json_normalize(lista_crew)

# filtra quem é o diretor de cada um dos filmes e adiciona no df 'credits'
direcao=pd.DataFrame({
    "titulo":0,
    "nome": 0,
    "cargo":0,
    "id":0
    },index=[0])
c=0
for i in range(df_crew.shape[0]):
    if df_crew["job"][i]=="Director" : 
        direcao.loc[c]=[df_crew["title"][i],df_crew["name"][i],df_crew["job"][i],df_crew["id"][i]]
        c+=1
credits=direcao.copy()
credits.to_csv(r"C:\Users\gabri\Case Chat Class\data-case-v1-master\tmdb_5000_credits.csv",index=False)


# SQL

# conecta python com do DataBase
sql_conn=sqlite3.connect("SQL.db")

# seleciona a tabela 'movies'
df= pd.read_sql_query('SELECT * from tmdb_5000_movies', sql_conn)

# tabela com budget individual para os diretores em cada um dos filmes
budget=pd.DataFrame({
    "nome": 0,
    "budget":0,
    },index=[0])
d=0
for j in range(df.shape[0]):
    for i in range(credits.shape[0]):
        if credits["titulo"][i]== df["title"][j] : 
            budget.loc[d]=[credits["nome"][i],df["budget"][j]]
            d+=1
            
# agrupamento dos diretores e a soma total dos budgets para cada um 
budget_total=pd.DataFrame(budget).groupby(["nome"]).budget.sum()
budget_total=budget_total.reset_index()    

# API rest
atividade = requests.get("http://www.boredapi.com/api/activity/")
atividade=atividade.json()
print (atividade)
