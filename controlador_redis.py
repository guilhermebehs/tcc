#!/usr/bin/python
# -*- coding: utf-8 -*-


import redis





def iniciarClienteRedis():
	r = redis.Redis()
	return r
    

def retornarChaves(expressaoRegular):
   resultado = {}
   for key in clienteRedis.scan_iter(expressaoRegular):
   	   resultado[key] = clienteRedis.hgetall(key)
   
   return resultado

def mergeResultados(resultado, dictsParaMerge):
    for dic in dictsParaMerge:
        resultado.update(dic)
    return resultado

def Q1S():
   return retornarChaves('*')

def Q2S():
    minorias = ['Preta', 'Indigena', 'Amarela', 'Parda']

    resultados=[]
    for minoria in minorias:
        retorno = retornarChaves("*TP_COR_RACA:"+minoria+"*")
        if len(retorno) > 0:
            resultados.append(retorno)

    if len(resultados) > 0:
      return mergeResultados(resultados[0], resultados)
    else:
        return {}


def Q3S():
    return retornarChaves("*IN_DEFICIENCIA_*:1*-TP_PRESENCA_CN*")

def Q4S():
    return retornarChaves("*TP_NACIONALIDADE:Brasileiro*")

clienteRedis = iniciarClienteRedis()

def main ():
    resultados = Q4S()
  #  for key in resultados:
  #      print(key)
    print('\n\n'+str(len(resultados))+' resultado(s)')



if __name__ == '__main__':
    main()