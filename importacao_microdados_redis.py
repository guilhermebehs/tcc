#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import csv
import unicodedata
import redis

dicIdAtributo = {"TP_COR_RACA_0":"Nao declarado", "TP_COR_RACA_1":"Branca", "TP_COR_RACA_2":"Preta", 
				         "TP_COR_RACA_3":"Parda", "TP_COR_RACA_4":"Amarela", "TP_COR_RACA_5":"Indigena",
                 "TP_NACIONALIDADE_0":"Nao informado", "TP_NACIONALIDADE_1": "Brasileiro(a)", 
                 "TP_NACIONALIDADE_2": "Brasileiro(a) Naturalizado(a)", "TP_NACIONALIDADE_3":"Estrangeiro(a)",
                 "TP_NACIONALIDADE_4":"Brasileiro(a) Nato(a), nascido no exterior"}


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




def resolverIdAtributo(coluna, valor):
	chave = coluna+'_'+valor
	if chave in dicIdAtributo:
		return dicIdAtributo[chave]
	else:
		return valor


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

    global clienteRedis

    clienteRedis = iniciarClienteRedis()


    with open('/home/guilhermebehs/Downloads/Microdados Enem 2017/DADOS/MICRODADOS_ENEM_2017.csv') as csv_file:
      data = csv.reader(csv_file, delimiter=";")
      count = 0
      for linha in data:
          if count == 0:
             colunas = linha
	            
          else:
             inserirRedis(linha, colunas)

          count= count+1
	 

    with open('/home/guilhermebehs/Downloads/microdados_enem2018/DADOS/MICRODADOS_ENEM_2018.csv') as csv_file:
      data = csv.reader(csv_file, delimiter=";")
      count = 0
      for linha in data:
          if count == 0:
             colunas = linha
	            
          else:
             inserirRedis(linha, colunas)

          count= count+1


    with open('/home/guilhermebehs/Downloads/microdados_enem_2019/DADOS/MICRODADOS_ENEM_2019.csv') as csv_file:
      data = csv.reader(csv_file, delimiter=";")
      count = 0
      for linha in data:
          if count == 0:
             colunas = linha
	            
          else:
             inserirRedis(linha, colunas)

          count= count+1
   

if __name__ == '__main__':
    main()