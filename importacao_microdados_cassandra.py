#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import csv
from cassandra.cluster import Cluster 

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



criouTabelaCassandra = False

colunasParaCassandra= []

idCassandra = 0;


def resolverIdAtributo(coluna, valor):
	chave = coluna+'_'+valor
	if chave in dicIdAtributo:
		return dicIdAtributo[chave]
	else:
		return valor



def iniciarClienteCassandra():
    clstr=Cluster()
    session=clstr.connect()
    session.execute("DROP keyspace if exists tcc;")
    session.execute("create keyspace tcc with replication={'class': 'SimpleStrategy', 'replication_factor' : 3};")
    session=clstr.connect("tcc")
    return session





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

        queryCriacaoTabela = queryCriacaoTabela+ "PRIMARY KEY ((id),NU_NOTA_REDACAO,NU_ANO,NU_NOTA_MT));"
        
        criouTabelaCassandra = True

        clienteCassandra.execute(queryCriacaoTabela)

        clienteCassandra.execute("CREATE CUSTOM INDEX likeIndex ON microdados (TP_NACIONALIDADE)"+
         "USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = "+
         "{'mode': 'CONTAINS', 'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer',"+
         " 'case_sensitive': 'false'};")

        for i in range(len(indexes)):
            clienteCassandra.execute("CREATE INDEX idx_"+indexes[i]+" ON tcc.microdados("+indexes+");")



            
    queryInsercao = " INSERT INTO microdados "
    queryCampos = " (id,"
    idCassandra =idCassandra+1
    queryValores = " VALUES("+str(idCassandra)+","
     

    for i in range(len(linha)):
        queryCampos = queryCampos + coluna[i]+","

        if linha[i].strip() == '':
            if coluna[i] in ['NU_NOTA_REDACAO','NU_NOTA_MT'] :
               queryValores = queryValores +'-1'+","
            else:
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
	
	




def main ():

    global colunasParaCassandra

    with open('/home/guilhermebehs/Downloads/MICRODADOS_ENEM_2017.csv') as csv_file:
	    data = csv.reader(csv_file, delimiter=";")
	    for linha in data:
	        colunasParaCassandra = linha
	        break

    with open('/home/guilhermebehs/Downloads/MICRODADOS_ENEM_2018.csv') as csv_file:
	    data = csv.reader(csv_file, delimiter=";")
	    for linha in data:
	        colunas2018 = linha
	        for coluna in colunas2018:
	            if coluna not in colunasParaCassandra:
	               colunasParaCassandra.append(coluna)  
	        break

    with open('/home/guilhermebehs/Downloads/MICRODADOS_ENEM_2019.csv') as csv_file:
	    data = csv.reader(csv_file, delimiter=";")
	    for linha in data:
	        colunas2019 = linha
	        for coluna in colunas2019:
	            if coluna not in colunasParaCassandra:
	               colunasParaCassandra.append(coluna)  
	        break


    global clienteCassandra
    clienteCassandra = iniciarClienteCassandra() 


    with open('/home/guilhermebehs/Downloads/MICRODADOS_ENEM_2017.csv') as csv_file:
	    data = csv.reader(csv_file, delimiter=";")
	    count = 0
	    for linha in data:
	        if count == 0:
	           colunas = linha
	            
	        else:
	           inserirCassandra(linha, colunas)
	        count= count+1
	        if count > 500 :
	           break

    with open('/home/guilhermebehs/Downloads/MICRODADOS_ENEM_2018.csv') as csv_file:
	    data = csv.reader(csv_file, delimiter=";")
	    count = 0
	    for linha in data:
	        if count == 0:
	           colunas = linha
	            
	        else:
	           inserirCassandra(linha, colunas)

	        count= count+1
	        if count > 500 :
	           break

    with open('/home/guilhermebehs/Downloads/MICRODADOS_ENEM_2019.csv') as csv_file:
	    data = csv.reader(csv_file, delimiter=";")
	    count = 0
	    for linha in data:
	        if count == 0:
	           colunas = linha
	            
	        else:
	           inserirCassandra(linha, colunas)

	        count= count+1
	        if count > 500 :
	           break
    

if __name__ == '__main__':
    main()