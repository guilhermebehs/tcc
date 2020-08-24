#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import csv
import unicodedata
import pymongo

dicIdAtributo = {"TP_COR_RACA_0":"Nao declarado", "TP_COR_RACA_1":"Branca", "TP_COR_RACA_2":"Preta", 
				 "TP_COR_RACA_3":"Parda", "TP_COR_RACA_4":"Amarela", "TP_COR_RACA_5":"Indigena",
                 "TP_NACIONALIDADE_0":"Nao informado", "TP_NACIONALIDADE_1": "Brasileiro(a)", 
                 "TP_NACIONALIDADE_2": "Brasileiro(a) Naturalizado(a)", "TP_NACIONALIDADE_3":"Estrangeiro(a)",
                 "TP_NACIONALIDADE_4":"Brasileiro(a) Nato(a), nascido no exterior"
				 }

converterInt = {"Q005":1}

indexes = [
           "NU_ANO",
           "SG_UF_RESIDENCIA", 
           "NU_IDADE", 
           "TP_NACIONALIDADE", 
           "TP_PRESENCA_CN",
           "TP_PRESENCA_CH", 
           "TP_PRESENCA_LC", 
           "TP_PRESENCA_MT", 
           "TX_RESPOSTAS_LC",
           "TP_LINGUA",
           "TX_GABARITO_LC",
           "IN_DEFICIENCIA_FISICA", 
           "IN_DEFICIENCIA_MENTAL", 
           "NU_NOTA_REDACAO",
           "NU_NOTA_MT", 
           "TP_COR_RACA", 
           "Q005"
           ]


arrayInsert =[]


def resolverIdAtributo(coluna, valor):
	chave = coluna+'_'+valor
	if chave in dicIdAtributo:
		return dicIdAtributo[chave]
	else:
		return valor


def iniciarClienteMongoDB ():
    clienteMongo = pymongo.MongoClient("mongodb://localhost:27017/")
    baseEstudo = clienteMongo["tcc"]
    colecaoMicrodados = baseEstudo["microdados"]
    colecaoMicrodados.drop()
    colecaoMicrodados = baseEstudo["microdados"]
    for index in indexes:
        colecaoMicrodados.create_index(index)
    return colecaoMicrodados


def inserirMongoDB (linha, coluna):

    documento = {}

    global arrayInsert

    for i in range(len(linha)):
        if linha[i].strip() == '':
            documento[coluna[i]] = None
        else:
            documento[coluna[i]] = resolverIdAtributo(coluna[i],linha[i].decode('latin-1').strip())  
            if 'IN_' in coluna[i]:
                   documento[coluna[i]] = bool(int(documento[coluna[i]]))
            if 'NU_' in coluna[i]:
                if documento[coluna[i]].isdigit():
                   documento[coluna[i]] = int(documento[coluna[i]])
                else:
                   documento[coluna[i]] = float(documento[coluna[i]])
            if coluna[i] in converterInt:
                documento[coluna[i]] = int(documento[coluna[i]]) 
    
    arrayInsert.append(documento)
    
    if len(arrayInsert) == 100000:
       clienteMongo.insert_many(arrayInsert)
       arrayInsert = []



def main ():

    global clienteMongo
    global arrayInsert

    clienteMongo = iniciarClienteMongoDB()


    with open('/home/guilhermebehs/Downloads/MICRODADOS_ENEM_2017.csv') as csv_file:
      data = csv.reader(csv_file, delimiter=";")
      count = 0
      for linha in data:
          if count == 0:
             colunas = linha
	            
          else:
             inserirMongoDB(linha, colunas)

          count= count+1
	 

    with open('/home/guilhermebehs/Downloads/MICRODADOS_ENEM_2018.csv') as csv_file:
      data = csv.reader(csv_file, delimiter=";")
      count = 0
      for linha in data:
          if count == 0:
             colunas = linha
	            
          else:
             inserirMongoDB(linha, colunas)

          count= count+1



    with open('/home/guilhermebehs/Downloads/MICRODADOS_ENEM_2019.csv') as csv_file:
      data = csv.reader(csv_file, delimiter=";")
      count = 0
      for linha in data:
          if count == 0:
             colunas = linha
	            
          else:
             inserirMongoDB(linha, colunas)

          count= count+1
          

    if len(arrayInsert) > 0:
    	clienteMongo.insert_many(arrayInsert)


if __name__ == '__main__':
    main()