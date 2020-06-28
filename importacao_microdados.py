#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import csv
import pymongo
import redis
from cassandra.cluster import Cluster 

dicIdAtributo = {"TP_COR_RACA_0":"Nao declarado", "TP_COR_RACA_1":"Branca", "TP_COR_RACA_2":"Preta", 
				 "TP_COR_RACA_3":"Parda", "TP_COR_RACA_4":"Amarela", "TP_COR_RACA_5":"Indigena",
				 "TP_ESTADO_CIVIL_0":"Solteiro(a)", "TP_ESTADO_CIVIL_1":"Casado(a)", "TP_ESTADO_CIVIL_2":"Em uniao estavel", 
				 "TP_ESTADO_CIVIL_3":"Mora com um(a) companheiro(a)", "TP_ESTADO_CIVIL_4":"Divorciado(a)", "TP_ESTADO_CIVIL_5":"Desquitado(a)", 
				 "TP_ESTADO_CIVIL_6":"Viuvo(a)",
				 "Q04_A": "1", "Q04_B": "1-3", "Q04_C": "3-6", "Q04_D": "6-9", "Q04_E": "9-12", "Q04_F": "12-15", "Q04_G": ">15","Q04_H":"0",
				 "Q05_A": "1", "Q05_B": "1-3", "Q05_C": "3-6", "Q05_D": "6-9", "Q05_E": "9-12", "Q05_F": "12-15", "Q05_G": ">15","Q05_H":"0",
				 }

criouTabelaCassandra = False


def resolverIdAtributo(coluna, valor):
	chave = coluna+'_'+valor
	if chave in dicIdAtributo:
		return dicIdAtributo[chave]
	else:
		return valor


def iniciarClienteMongoDB ():
    clienteMongo = pymongo.MongoClient("mongodb://localhost:27017/")
    baseEstudo = clienteMongo["estudo"]
    colecaoMicrodados = baseEstudo["microdados"]
    colecaoMicrodados.drop()
    colecaoMicrodados = baseEstudo["microdados"]
    return colecaoMicrodados

def iniciarClienteCassandra():
    clstr=Cluster()
    session=clstr.connect()
    session.execute("DROP keyspace if exists estudo;")
    session.execute("create keyspace estudo with replication={'class': 'SimpleStrategy', 'replication_factor' : 3};")
    session=clstr.connect("estudo")
    return session

def iniciarClienteRedis():
	r = redis.Redis()
	return r


def inserirMongoDB (linha, coluna):

    documento = {}

    for i in range(len(linha)):
    	if linha[i].strip() == '':
    	    documento[coluna[i]] = None
        else:
            documento[coluna[i]] = resolverIdAtributo(coluna[i],linha[i].strip())    

    clienteMongo.insert_one(documento)


def inserirCassandra (linha, coluna):
    global criouTabelaCassandra
    if criouTabelaCassandra == False :
        clienteCassandra.execute("DROP table if exists microdados;")
        queryCriacaoTabela = "create table microdados ("
        for i in range(len(coluna)):
            queryCriacaoTabela = queryCriacaoTabela +" "+ coluna[i]
            if coluna[i] == "NU_INSCRICAO":
               queryCriacaoTabela = queryCriacaoTabela + " BIGINT PRIMARY KEY, "
            elif "NU_" in  coluna[i]:
               queryCriacaoTabela = queryCriacaoTabela + " DECIMAL,"
            else :
               queryCriacaoTabela = queryCriacaoTabela + " VARCHAR,"

        queryCriacaoTabela = queryCriacaoTabela+");"
        queryCriacaoTabela = queryCriacaoTabela.replace(",)", ")")
        criouTabelaCassandra = True
        clienteCassandra.execute(queryCriacaoTabela)


    else:
        queryInsercao = " INSERT INTO microdados "
        queryCampos = " ("
        queryValores = " VALUES("
        for i in range(len(linha)):
            queryCampos = queryCampos + coluna[i]+","

            if linha[i].strip() == '':
                queryValores = queryValores +'null'+","
            elif "NU_" in coluna[i]:
                queryValores = queryValores +linha[i].strip()+","
            else:
            	queryValores = queryValores +"'"+linha[i].strip()+"',"

        queryValores = queryValores+");"
        queryCampos =queryCampos+")"
        queryValores = queryValores.replace(",)", ")")
        queryCampos = queryCampos.replace(",)", ")")
        queryInsercao = queryInsercao + queryCampos + queryValores
        clienteCassandra.execute(queryInsercao)
	
	

def inserirRedis(linha, coluna):

 
    for i in range(len(linha)):
    	if linha[i] == '':
    	   linha[i] = 'null' 
    	else :   
    	   linha[i] = resolverIdAtributo(coluna[i], linha[i])

    chave = linha[0]+'-'+linha[1]+'-'+linha[6]+'-'+linha[7]+'-'+linha[9]

    for i in range(len(linha)):  
        clienteRedis.hset(chave, coluna[i], linha[i])




clienteMongo = iniciarClienteMongoDB()
clienteCassandra = iniciarClienteCassandra()
clienteRedis = iniciarClienteRedis()


def main ():

 

    with open('MICRODADOS_ENEM_2010.csv') as csv_file:
	    data = csv.reader(csv_file, delimiter=";")
	    count = 0
	    for linha in data:
	        if count == 0:
	           colunas = linha
	            
	        else:
	           inserirMongoDB(linha, colunas)
	           inserirCassandra(linha, colunas)
	           inserirRedis(linha, colunas)

	        count= count+1
	        if count > 50 :
	           break



if __name__ == '__main__':
    main()