#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import csv
import unicodedata
import pymongo
import redis
from cassandra.cluster import Cluster 

dicIdAtributo = {"TP_COR_RACA_0":"Nao declarado", "TP_COR_RACA_1":"Branca", "TP_COR_RACA_2":"Preta", 
				 "TP_COR_RACA_3":"Parda", "TP_COR_RACA_4":"Amarela", "TP_COR_RACA_5":"Indigena",
                 "TP_NACIONALIDADE_0":"Nao informado", "TP_NACIONALIDADE_1": "Brasileiro(a)", 
                 "TP_NACIONALIDADE_2": "Brasileiro(a) Naturalizado(a)", "TP_NACIONALIDADE_3":"Estrangeiro(a)",
                 "TP_NACIONALIDADE_4":"Brasileiro(a) Nato(a), nascido no exterior"
				 }

converterInt = {"Q005":1}

indexes = ["NU_INSCRICAO","NU_ANO","SG_UF_RESIDENCIA", "NU_IDADE", "TP_NACIONALIDADE", "TP_PRESENCA_CN",
           "TP_PRESENCA_CH", "TP_PRESENCA_LC", "TP_PRESENCA_MT", "TX_RESPOSTAS_LC","TP_LINGUA","TX_GABARITO_LC",
           "IN_DEFICIENCIA_FISICA", "IN_DEFICIENCIA_MENTAL", "NU_NOTA_REDACAO", "NU_NOTA_MT", "TP_COR_RACA"]



criouTabelaCassandra = False

colunasParaCassandra= []

idCassandra = 0;


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

def iniciarClienteCassandra():
    clstr=Cluster()
    session=clstr.connect()
    session.execute("DROP keyspace if exists tcc;")
    session.execute("create keyspace tcc with replication={'class': 'SimpleStrategy', 'replication_factor' : 3};")
    session=clstr.connect("tcc")
    return session

def iniciarClienteRedis():
	r = redis.Redis()
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


def inserirCassandra (linha, coluna):

    global idCassandra
    global criouTabelaCassandra
    if criouTabelaCassandra == False :
        clienteCassandra.execute("DROP table if exists microdados;")
        queryCriacaoTabela = "create table microdados ("
        queryCriacaoTabela = queryCriacaoTabela + " id BIGINT, "
        for i in range(len(colunasParaCassandra)):
            queryCriacaoTabela = queryCriacaoTabela +" "+ colunasParaCassandra[i]
            if "NU_" in  colunasParaCassandra[i]:
               queryCriacaoTabela = queryCriacaoTabela + " DECIMAL,"
            elif 'IN_' in colunasParaCassandra[i]:
                queryCriacaoTabela = queryCriacaoTabela + " BOOLEAN,"
            elif colunasParaCassandra[i] in converterInt:
                queryCriacaoTabela = queryCriacaoTabela + " INT,"
            else :
               queryCriacaoTabela = queryCriacaoTabela + " VARCHAR,"

        queryCriacaoTabela = queryCriacaoTabela+ "PRIMARY KEY ((id),TP_COR_RACA,NU_ANO ,TP_NACIONALIDADE));"
        
        criouTabelaCassandra = True

        clienteCassandra.execute(queryCriacaoTabela)

         
        for index in indexes:
            if index != 'NU_ANO' and index != 'TP_NACIONALIDADE':
                queryCriacaoIndexes = " CREATE INDEX ON tcc.microdados ("+index+"); "
                clienteCassandra.execute(queryCriacaoIndexes)
        
            
    queryInsercao = " INSERT INTO microdados "
    queryCampos = " (id,"
    idCassandra =idCassandra+1
    queryValores = " VALUES("+str(idCassandra)+","
     

    for i in range(len(linha)):
        queryCampos = queryCampos + coluna[i]+","

        if linha[i].strip() == '':
            queryValores = queryValores +'null'+","
        elif "NU_" in coluna[i]  or coluna[i] in converterInt:
            queryValores = queryValores +linha[i].strip()+"," 
        elif 'IN_' in coluna[i]:
            queryValores = queryValores + str(bool(int(linha[i])))+','
        else:
            valorFormatado = linha[i].decode('latin-1').strip().replace("'","")
            valorFormatado = valorFormatado.encode('utf8')
            queryValores = queryValores +"'"+resolverIdAtributo(coluna[i],valorFormatado)+"',"

    queryValores = queryValores+");"
    queryCampos =queryCampos+")"
    queryValores = queryValores.replace(",)", ")")
    queryCampos = queryCampos.replace(",)", ")")
    queryInsercao = queryInsercao + queryCampos + queryValores
    clienteCassandra.execute(queryInsercao)
	
	

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

    global colunasParaCassandra

    with open('MICRODADOS_ENEM_2017.csv') as csv_file:
	    data = csv.reader(csv_file, delimiter=";")
	    for linha in data:
	        colunasParaCassandra = linha
	        break

    with open('MICRODADOS_ENEM_2018.csv') as csv_file:
	    data = csv.reader(csv_file, delimiter=";")
	    for linha in data:
	        colunas2018 = linha
	        for coluna in colunas2018:
	            if coluna not in colunasParaCassandra:
	               colunasParaCassandra.append(coluna)  
	        break

    with open('MICRODADOS_ENEM_2019.csv') as csv_file:
	    data = csv.reader(csv_file, delimiter=";")
	    for linha in data:
	        colunas2019 = linha
	        for coluna in colunas2019:
	            if coluna not in colunasParaCassandra:
	               colunasParaCassandra.append(coluna)  
	        break



    global clienteMongo
    global clienteCassandra
    global clienteRedis
    #clienteMongo = iniciarClienteMongoDB()
    #clienteCassandra = iniciarClienteCassandra() 
    clienteRedis = iniciarClienteRedis()


    with open('MICRODADOS_ENEM_2017.csv') as csv_file:
	    data = csv.reader(csv_file, delimiter=";")
	    count = 0
	    for linha in data:
	        if count == 0:
	           colunas = linha
	            
	        else:
#	           inserirMongoDB(linha, colunas)
#	           inserirCassandra(linha, colunas)
	           inserirRedis(linha, colunas)

	        count= count+1
	        if count > 500 :
	           break

    with open('MICRODADOS_ENEM_2018.csv') as csv_file:
	    data = csv.reader(csv_file, delimiter=";")
	    count = 0
	    for linha in data:
	        if count == 0:
	           colunas = linha
	            
	        else:
#	           inserirMongoDB(linha, colunas)
#	           inserirCassandra(linha, colunas)
	           inserirRedis(linha, colunas)

	        count= count+1
	        if count > 500 :
	           break

    with open('MICRODADOS_ENEM_2019.csv') as csv_file:
	    data = csv.reader(csv_file, delimiter=";")
	    count = 0
	    for linha in data:
	        if count == 0:
	           colunas = linha
	            
	        else:
#	           inserirMongoDB(linha, colunas)
#	           inserirCassandra(linha, colunas)
	           inserirRedis(linha, colunas)

	        count= count+1
	        if count > 500 :
	           break
    

if __name__ == '__main__':
    main()