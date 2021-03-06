#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import csv
import unicodedata
import pymongo
import redis

dicIdAtributo = {"TP_COR_RACA_0":"Nao declarado", "TP_COR_RACA_1":"Branca", "TP_COR_RACA_2":"Preta", 
				 "TP_COR_RACA_3":"Parda", "TP_COR_RACA_4":"Amarela", "TP_COR_RACA_5":"Indigena",
                 "TP_NACIONALIDADE_0":"Nao informado", "TP_NACIONALIDADE_1": "Brasileiro(a)", 
                 "TP_NACIONALIDADE_2": "Brasileiro(a) Naturalizado(a)", "TP_NACIONALIDADE_3":"Estrangeiro(a)",
                 "TP_NACIONALIDADE_4":"Brasileiro(a) Nato(a), nascido no exterior"
				 }

converterInt = {"Q005":1}

indexes = ["NU_INSCRICAO","NU_ANO","SG_UF_RESIDENCIA", "NU_IDADE", "TP_NACIONALIDADE", "TP_PRESENCA_CN",
           "TP_PRESENCA_CH", "TP_PRESENCA_LC", "TP_PRESENCA_MT", "TX_RESPOSTAS_LC","TP_LINGUA","TX_GABARITO_LC",
           "IN_DEFICIENCIA_FISICA", "IN_DEFICIENCIA_MENTAL", "NU_NOTA_REDACAO", "NU_NOTA_MT", "TP_COR_RACA", "Q005"]





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



def iniciarClienteRedis():
  r = redis.Redis()
  r.flushdb()
  return r

def retirarAcentosRedis(text):


    try:
        text = unicode(text, 'utf-8')
    except NameError: 
        pass
    except TypeError: 
        pass


    text = unicodedata.normalize('NFD', text)\
           .encode('ascii', 'ignore')\
           .decode("utf-8")


    return str(text)


def inserirMongoDB (linha, coluna):

    documento = {}

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



    clienteMongo.insert_one(documento)




def inserirRedis(linha, coluna):

    chave= ''
    for i in range(len(linha)):
        if linha[i] == '':
           linha[i] = 'null' 
        else :   
           linha[i] = resolverIdAtributo(coluna[i], linha[i])
        if coluna[i] in indexes:
           valorFormatado = linha[i].decode('latin-1').strip().replace(' ','_')
           valorFormatado = retirarAcentosRedis(valorFormatado)
           if valorFormatado == 'IndAgena':
              valorFormatado = 'Indigena'
           chave = chave+coluna[i]+':'+valorFormatado+"-"

    for i in range(len(linha)):  
        valorFormatado = linha[i].decode('latin-1').strip()
        valorFormatado = retirarAcentosRedis(valorFormatado)
        if valorFormatado == 'IndAgena':
           valorFormatado = 'Indigena'
        clienteRedis.hset(chave[0:len(chave)-1], coluna[i], valorFormatado)






def main ():

    

    global clienteMongo
    global clienteRedis
    clienteMongo = iniciarClienteMongoDB()
    clienteRedis = iniciarClienteRedis()


    with open('/home/guilhermebehs/Downloads/MICRODADOS_ENEM_2017.csv') as csv_file:
      data = csv.reader(csv_file, delimiter=";")
      count = 0
      for linha in data:
          if count == 0:
             colunas = linha
	            
          else:
             inserirMongoDB(linha, colunas)
             inserirRedis(linha, colunas)

          count= count+1

          if count > 500:
            break
	 

    with open('/home/guilhermebehs/Downloads/MICRODADOS_ENEM_2018.csv') as csv_file:
      data = csv.reader(csv_file, delimiter=";")
      count = 0
      for linha in data:
          if count == 0:
             colunas = linha
	            
          else:
             inserirMongoDB(linha, colunas)
             inserirRedis(linha, colunas)

          count= count+1

          if count > 500:
            break


    with open('/home/guilhermebehs/Downloads/MICRODADOS_ENEM_2019.csv') as csv_file:
      data = csv.reader(csv_file, delimiter=";")
      count = 0
      for linha in data:
          if count == 0:
             colunas = linha
	            
          else:
             inserirMongoDB(linha, colunas)
             inserirRedis(linha, colunas)

          count= count+1

          if count > 500:
            break
    

if __name__ == '__main__':
    main()