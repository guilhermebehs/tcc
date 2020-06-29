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



def retornarPorRaca(racas):
    resultados=[]
    for raca in racas:
        resultados.append(retornarChaves("*-"+raca))

    if len(resultados) > 0:
    	return mergeResultados(resultados[0], resultados)
    else:
        return {}

#def retornarPorIntervaloAno():

#def retornarPorIntervaloAnoESexo():

clienteRedis = iniciarClienteRedis()

def main ():
    resultados = retornarPorRaca(["Parda", "Preta"])
    for resultado in resultados:
        print(resultado)



if __name__ == '__main__':
    main()