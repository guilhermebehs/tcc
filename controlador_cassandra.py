from cassandra.cluster import Cluster 


def iniciarClienteCassandra():
    clstr=Cluster()
    session=clstr.connect()
    session=clstr.connect("tcc")
    return session

def Q1S():
   query = "SELECT * FROM microdados;"
   return clienteCassandra.execute(query)

def Q2S():
   query = "SELECT NU_INSCRICAO FROM microdados_por_cor_raca WHERE TP_COR_RACA IN ('Preta', 'Indigena', 'Amarela', 'Parda') ALLOW FILTERING; "
   inscricoes = clienteCassandra.execute(query)	
   inscricoes = [item for t in inscricoes for item in t]
   resultadosFormatados = ','.join(str(x) for x in inscricoes)
   query = "SELECT * FROM microdados WHERE NU_INSCRICAO IN ("+resultadosFormatados+") ALLOW FILTERING;"
   inscricoes = clienteCassandra.execute(query)
   return inscricoes

def Q3S():
    query = "SELECT NU_INSCRICAO FROM microdados WHERE IN_DEFICIENCIA_FISICA = true ALLOW FILTERING; "
    inscricoesDefFisica = clienteCassandra.execute(query)
    inscricoesDefFisica = [item for t in inscricoesDefFisica for item in t]
    query = "SELECT NU_INSCRICAO FROM microdados WHERE IN_DEFICIENCIA_MENTAL = true ALLOW FILTERING; "
    inscricoesDefMental = clienteCassandra.execute(query)
    inscricoesDefMental = [item for t in inscricoesDefMental for item in t]
    inscricoesDef = list(set(inscricoesDefFisica) | set(inscricoesDefMental))
    inscricoesDef = ','.join(str(x) for x in inscricoesDef)
    query = "SELECT NU_INSCRICAO FROM microdados WHERE NU_INSCRICAO IN ("+inscricoesDef+") ALLOW FILTERING; "
    inscricoes = clienteCassandra.execute(query)
    return inscricoes

def Q4S():
    query = "SELECT * FROM microdados WHERE TP_NACIONALIDADE LIKE '*Brasileiro*' ALLOW FILTERING ;"
    inscricoes = clienteCassandra.execute(query)
    return inscricoes


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



if __name__ == '__main__':
    main()