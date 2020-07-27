from controlador_mongodb import ControladorMongoDB
from memory_profiler import profile
import time

def executarMongoDB():

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



def main():
    global controladorMongoDB
    controladorMongoDB = ControladorMongoDB()
    executarMongoDB()

if __name__ == '__main__':
    main()