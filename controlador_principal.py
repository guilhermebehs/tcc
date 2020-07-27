from controlador_mongodb import ControladorMongoDB
from controlador_cassandra import ControladorCassandra
from controlador_redis import ControladorRedis

from memory_profiler import profile
import time

def executarMongoDB():

     controladorMongoDB = ControladorMongoDB()

     start = time.time()
     controladorMongoDB.Q1S()
     end = time.time()
     Q1STempoExecucao = (end - start) 

     start = time.time()
     controladorMongoDB.Q2S()
     end = time.time()
     Q2STempoExecucao = (end - start) 

     start = time.time()
     controladorMongoDB.Q3S()
     end = time.time()
     Q3STempoExecucao = (end - start) 

     start = time.time()
     controladorMongoDB.Q4S()
     end = time.time()
     Q4STempoExecucao = (end - start) 

     start = time.time()
     controladorMongoDB.Q1M()
     end = time.time()
     Q1MTempoExecucao = (end - start) 

     start = time.time()
     controladorMongoDB.Q2M()
     end = time.time()
     Q2MTempoExecucao = (end - start) 

     start = time.time()
     controladorMongoDB.Q3M()
     end = time.time()
     Q3MTempoExecucao = (end - start) 

     start = time.time()
     controladorMongoDB.Q4M()
     end = time.time()
     Q4MTempoExecucao = (end - start) 

     start = time.time()
     controladorMongoDB.Q1C()
     end = time.time()
     Q1CTempoExecucao = (end - start) 

     start = time.time()
     controladorMongoDB.Q2C()
     end = time.time()
     Q2CTempoExecucao = (end - start) 

     start = time.time()
     controladorMongoDB.Q3C()
     end = time.time()
     Q3CTempoExecucao = (end - start) 

     start = time.time()
     controladorMongoDB.Q4C()
     end = time.time()
     Q4CTempoExecucao = (end - start) 


def executarCassandra():

     controladorCassandra = ControladorCassandra()

     start = time.time()
     controladorCassandra.Q1S()
     end = time.time()
     Q1STempoExecucao = (end - start) 

     start = time.time()
     controladorCassandra.Q2S()
     end = time.time()
     Q2STempoExecucao = (end - start) 

     start = time.time()
     controladorCassandra.Q3S()
     end = time.time()
     Q3STempoExecucao = (end - start) 

     start = time.time()
     controladorCassandra.Q4S()
     end = time.time()
     Q4STempoExecucao = (end - start) 

     start = time.time()
     controladorCassandra.Q1M()
     end = time.time()
     Q1MTempoExecucao = (end - start) 

     start = time.time()
     controladorCassandra.Q2M()
     end = time.time()
     Q2MTempoExecucao = (end - start) 

     start = time.time()
     controladorCassandra.Q3M()
     end = time.time()
     Q3MTempoExecucao = (end - start) 

     start = time.time()
     controladorCassandra.Q4M()
     end = time.time()
     Q4MTempoExecucao = (end - start) 

     start = time.time()
     controladorCassandra.Q1C()
     end = time.time()
     Q1CTempoExecucao = (end - start) 

     start = time.time()
     controladorCassandra.Q2C()
     end = time.time()
     Q2CTempoExecucao = (end - start) 

     start = time.time()
     controladorCassandra.Q3C()
     end = time.time()
     Q3CTempoExecucao = (end - start) 

     start = time.time()
     controladorCassandra.Q4C()
     end = time.time()
     Q4CTempoExecucao = (end - start) 


def executarRedis():

     controladorRedis = ControladorRedis()

     start = time.time()
     controladorRedis.Q1S()
     end = time.time()
     Q1STempoExecucao = (end - start) 

     start = time.time()
     controladorRedis.Q2S()
     end = time.time()
     Q2STempoExecucao = (end - start) 

     start = time.time()
     controladorRedis.Q3S()
     end = time.time()
     Q3STempoExecucao = (end - start) 

     start = time.time()
     controladorRedis.Q4S()
     end = time.time()
     Q4STempoExecucao = (end - start) 

     start = time.time()
     controladorRedis.Q1M()
     end = time.time()
     Q1MTempoExecucao = (end - start) 

     start = time.time()
     controladorRedis.Q2M()
     end = time.time()
     Q2MTempoExecucao = (end - start) 

     start = time.time()
     controladorRedis.Q3M()
     end = time.time()
     Q3MTempoExecucao = (end - start) 

     start = time.time()
     controladorRedis.Q4M()
     end = time.time()
     Q4MTempoExecucao = (end - start) 

     start = time.time()
     controladorRedis.Q1C()
     end = time.time()
     Q1CTempoExecucao = (end - start) 

     start = time.time()
     controladorRedis.Q2C()
     end = time.time()
     Q2CTempoExecucao = (end - start) 

     start = time.time()
     controladorRedis.Q3C()
     end = time.time()
     Q3CTempoExecucao = (end - start) 

     start = time.time()
     controladorRedis.Q4C()
     end = time.time()
     Q4CTempoExecucao = (end - start) 


def main():
     #executarMongoDB()
     #executarCassandra()
     executarRedis()

if __name__ == '__main__':
    main()