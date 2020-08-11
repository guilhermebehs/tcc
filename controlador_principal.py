#!/usr/bin/python
# -*- coding: utf-8 -*-

from controlador_mongodb import ControladorMongoDB
from controlador_cassandra import ControladorCassandra
from controlador_redis import ControladorRedis
import time


def executar(objeto,nomeFunction):
    tempoExecucao = 0 
    print("\n\n"+nomeFunction+":\n")

    for i in range(10):
         start = time.time()
         getattr(objeto, nomeFunction)()
         end = time.time()
         tempoExecucao = tempoExecucao + (end - start) 
    print("Tempo médio de execução: "+str(tempoExecucao/10))


def executarMongoDB():

     controladorMongoDB = ControladorMongoDB()

     print("========== Iniciando o teste com o MongoDB ==========\n\n")

     executar(controladorMongoDB,"Q1S")
     executar(controladorMongoDB,"Q2S")
     executar(controladorMongoDB,"Q3S")
     executar(controladorMongoDB,"Q4S")
     executar(controladorMongoDB,"Q1M")
     executar(controladorMongoDB,"Q2M")
     executar(controladorMongoDB,"Q3M")
     executar(controladorMongoDB,"Q4M")
     executar(controladorMongoDB,"Q1C")
     executar(controladorMongoDB,"Q2C")
     executar(controladorMongoDB,"Q3C")
     executar(controladorMongoDB,"Q4C")


def executarCassandra():

     controladorCassandra = ControladorCassandra()

     print("========== Iniciando o teste com o Apache Cassandra ==========\n\n")

     executar(controladorCassandra,"Q1S")
     executar(controladorCassandra,"Q2S")
     executar(controladorCassandra,"Q3S")
     executar(controladorCassandra,"Q4S")
     executar(controladorCassandra,"Q1M")
     executar(controladorCassandra,"Q2M")
     executar(controladorCassandra,"Q3M")
     executar(controladorCassandra,"Q4M")
     executar(controladorCassandra,"Q1C")
     executar(controladorCassandra,"Q2C")
     executar(controladorCassandra,"Q3C")
     executar(controladorCassandra,"Q4C")

    

def executarRedis():

     controladorRedis = ControladorRedis()

     print("========== Iniciando o teste com o Redis ==========\n\n")

     executar(controladorRedis,"Q1S")
     executar(controladorRedis,"Q2S")
     executar(controladorRedis,"Q3S")
     executar(controladorRedis,"Q4S")
     executar(controladorRedis,"Q1M")
     executar(controladorRedis,"Q2M")
     executar(controladorRedis,"Q3M")
     executar(controladorRedis,"Q4M")
     executar(controladorRedis,"Q1C")
     executar(controladorRedis,"Q2C")
     executar(controladorRedis,"Q3C")
     executar(controladorRedis,"Q4C")


def main():
     executarMongoDB()
     executarCassandra()
     executarRedis()

if __name__ == '__main__':
    main()