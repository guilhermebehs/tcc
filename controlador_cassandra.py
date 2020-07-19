from cassandra.cluster import Cluster 
from cassandra.query import SimpleStatement

def iniciarClienteCassandra():
    clstr=Cluster()
    session=clstr.connect("tcc")
    session.row_factory
    return session

def Q1S():
   query = "SELECT * FROM microdados;"
   return clienteCassandra.execute(query)

def Q2S():
   query = "SELECT id FROM microdados_por_cor_raca WHERE TP_COR_RACA IN ('Preta', 'Indigena', 'Amarela', 'Parda') ALLOW FILTERING; "
   inscricoes = clienteCassandra.execute(query)	
   inscricoes = [item for t in inscricoes for item in t]
   resultadosFormatados = ','.join(str(x) for x in inscricoes)
   query = "SELECT * FROM microdados WHERE id IN ("+resultadosFormatados+") ALLOW FILTERING;"
   inscricoes = clienteCassandra.execute(query)
   return inscricoes


def Q3S():
    query = "SELECT id FROM microdados WHERE IN_DEFICIENCIA_FISICA = true ALLOW FILTERING; "
    inscricoesDefFisica = clienteCassandra.execute(query)
    inscricoesDefFisica = [item for t in inscricoesDefFisica for item in t]
    query = "SELECT id FROM microdados WHERE IN_DEFICIENCIA_MENTAL = true ALLOW FILTERING; "
    inscricoesDefMental = clienteCassandra.execute(query)
    inscricoesDefMental = [item for t in inscricoesDefMental for item in t]
    inscricoesDef = list(set(inscricoesDefFisica) | set(inscricoesDefMental))
    inscricoesDef = ','.join(str(x) for x in inscricoesDef)
    query = "SELECT * FROM microdados WHERE id IN ("+inscricoesDef+") ALLOW FILTERING; "
    inscricoes = clienteCassandra.execute(query)
    return inscricoes

def Q4S():
    query = "SELECT * FROM microdados WHERE TP_NACIONALIDADE LIKE '*Brasileiro*' ALLOW FILTERING ;"
    inscricoes = clienteCassandra.execute(query)
    return inscricoes

def Q1M():
    query = " SELECT NU_NOTA_REDACAO FROM microdados WHERE NU_ANO=2019 and NU_NOTA_REDACAO > -1  ALLOW FILTERING;"
    notas = clienteCassandra.execute(query)
    notasOrdenadas = sorted(notas, reverse=True)  
    notasTop5 =[]
    for nota in notasOrdenadas:
        notasTop5.append(float(nota[0]))
        if len(notasTop5) == 5:
           break
    
    return notasTop5

def Q2M():
    query = "SELECT id FROM microdados_por_cor_raca WHERE TP_COR_RACA IN ('Preta', 'Indigena', 'Amarela', 'Parda') ALLOW FILTERING; "
    inscricoes = clienteCassandra.execute(query)	
    inscricoes = [item for t in inscricoes for item in t]
    resultadosFormatados = ','.join(str(x) for x in inscricoes)
    query= "SELECT NU_NOTA_REDACAO FROM microdados WHERE id in ("+resultadosFormatados+") AND NU_NOTA_REDACAO > -1 ALLOW FILTERING;"
    notas = clienteCassandra.execute(query)
    notasOrdenadas = sorted(notas, reverse=True)  
    notasTop5 =[]
    for nota in notasOrdenadas:
        notasTop5.append(float(nota[0]))
        if len(notasTop5) == 5:
           break
    
    return notasTop5

def Q3M():
    query = " SELECT NU_NOTA_REDACAO FROM microdados WHERE NU_IDADE >=15 AND NU_IDADE <=18 AND NU_NOTA_REDACAO > -1  ALLOW FILTERING;"
    notas = clienteCassandra.execute(query)
    notasOrdenadas = sorted(notas, reverse=True)  
    notasTop5 =[]
    for nota in notasOrdenadas:
        notasTop5.append(float(nota[0]))
        if len(notasTop5) == 5:
           break
    
    return notasTop5

def Q4M():
    query = " SELECT TX_GABARITO_LC,TX_RESPOSTAS_LC FROM microdados ALLOW FILTERING;"
    inscritos = clienteCassandra.execute(query)
    contagem= 0;
    for inscrito in inscritos:
        if inscrito[0] != None and inscrito[0][0:5] == inscrito[1][0:5]:
           contagem = contagem + 1
    
    return contagem



clienteCassandra = iniciarClienteCassandra()

def main ():

    resultados = Q1S()
    print('\n\n'+str(len(resultados.current_rows))+' resultado(s) de Q1')

    resultados = Q2S()
    print('\n\n'+str(len(resultados.current_rows))+' resultado(s) de Q2')

    resultados = Q3S()
    print('\n\n'+str(len(resultados.current_rows))+' resultado(s) de Q3')

    resultados = Q4S()
    print('\n\n'+str(len(resultados.current_rows))+' resultado(s) de Q4')

    resultados = Q1M()
    print(resultados)

    resultados = Q2M()
    print(resultados)

    resultados = Q3M()
    print(resultados)

    resultados = Q4M()
    print(resultados)

if __name__ == '__main__':
    main()