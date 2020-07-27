from cassandra.cluster import Cluster 
from cassandra.query import SimpleStatement

def iniciarClienteCassandra():
    clstr=Cluster()
    session=clstr.connect("tcc")
    return session

def ordenarAgrupamento(resultados):
    resultadosOrdenados =sorted(resultados.items(), key=lambda x:x[1], reverse=True)
    return resultadosOrdenados     


def Q1S():
   query = "SELECT * FROM microdados;"
   return list(clienteCassandra.execute(query))

def Q2S():
   query = "SELECT * FROM microdados WHERE TP_COR_RACA = 'Preta' ALLOW FILTERING; "
   inscricoesPreta = list(clienteCassandra.execute(query))	
   query = "SELECT * FROM microdados WHERE TP_COR_RACA = 'Parda' ALLOW FILTERING; "
   inscricoesParda = list(clienteCassandra.execute(query))	
   query = "SELECT * FROM microdados WHERE TP_COR_RACA = 'Amarela' ALLOW FILTERING; "
   inscricoesAmarela = list(clienteCassandra.execute(query))	
   query = "SELECT * FROM microdados WHERE TP_COR_RACA = 'Indigena' ALLOW FILTERING; "
   inscricoesIndigena = list(clienteCassandra.execute(query))	
   inscricoes = tuple(inscricoesPreta) + tuple(inscricoesIndigena) + tuple(inscricoesParda) + tuple(inscricoesAmarela)
   
   return list(inscricoes)


def Q3S():
    query = "SELECT * FROM microdados WHERE IN_DEFICIENCIA_FISICA = true ALLOW FILTERING; "
    inscricoesDefFisica = clienteCassandra.execute(query)
    query = "SELECT * FROM microdados WHERE IN_DEFICIENCIA_MENTAL = true ALLOW FILTERING; "
    inscricoesDefMental = clienteCassandra.execute(query)
    inscricoes = list(inscricoesDefFisica) + list(set(inscricoesDefMental) - set(inscricoesDefFisica))

    return inscricoes

def Q4S():
    query = "SELECT * FROM microdados WHERE TP_NACIONALIDADE LIKE '*Brasileiro*' ALLOW FILTERING ;"
    inscricoes = list(clienteCassandra.execute(query))
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
    query = "SELECT NU_NOTA_REDACAO FROM microdados WHERE TP_COR_RACA = 'Preta' AND NU_NOTA_REDACAO > -1 ALLOW FILTERING; "
    notasPreta = clienteCassandra.execute(query)	
    query = "SELECT NU_NOTA_REDACAO FROM microdados WHERE TP_COR_RACA = 'Parda' AND NU_NOTA_REDACAO > -1 ALLOW FILTERING; "
    notasParda = clienteCassandra.execute(query)	
    query = "SELECT NU_NOTA_REDACAO FROM microdados WHERE TP_COR_RACA = 'Amarela' AND NU_NOTA_REDACAO > -1 ALLOW FILTERING; "
    notasAmarela = clienteCassandra.execute(query)	
    query = "SELECT NU_NOTA_REDACAO FROM microdados WHERE TP_COR_RACA = 'Indigena' AND NU_NOTA_REDACAO > -1 ALLOW FILTERING; "
    notasIndigena = clienteCassandra.execute(query)	
    notas = tuple(notasPreta) + tuple(notasParda) + tuple(notasAmarela) + tuple(notasIndigena)

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

def Q1C():
    query = " SELECT NU_NOTA_REDACAO,TP_COR_RACA FROM microdados WHERE NU_ANO = 2018 AND NU_NOTA_REDACAO > -1 ALLOW FILTERING;"
    resultados = list(clienteCassandra.execute(query))
    agrupamentos ={}
    medias = {}
    for resultado in resultados:
        if resultado[0] != 'null':
           raca = resultado[1]
           nota = resultado[0]
           if raca in agrupamentos: 
              agrupamentos[raca].append(float(nota))
           else:
              agrupamentos[raca] = [float(nota)]
    
    
    for agrupamento in agrupamentos:
        media = sum(agrupamentos[agrupamento]) / len(agrupamentos[agrupamento])
        medias[agrupamento] = media

          
    return ordenarAgrupamento(medias)


def Q2C():
    query = " SELECT NU_NOTA_MT,NU_ANO FROM microdados WHERE Q005 >= 5 AND Q005 <= 9 AND NU_NOTA_MT > -1 ALLOW FILTERING;"
    resultados = list(clienteCassandra.execute(query))
    agrupamentos ={}
    medias = {}
    for resultado in resultados:
        if resultado[0] != 'null':
           ano = resultado[1]
           nota = resultado[0]
           if ano in agrupamentos: 
              agrupamentos[ano].append(float(nota))
           else:
              agrupamentos[ano] = [float(nota)]
    
    
    for agrupamento in agrupamentos:
        media = sum(agrupamentos[agrupamento]) / len(agrupamentos[agrupamento])
        medias[agrupamento] = media

          
    return ordenarAgrupamento(medias)


def Q3C():
    query = " SELECT SG_UF_RESIDENCIA FROM microdados WHERE TP_PRESENCA_CH = '0' AND TP_PRESENCA_CN = '0' AND TP_PRESENCA_LC = '0' AND TP_PRESENCA_MT= '0' ALLOW FILTERING;"
    resultados = list(clienteCassandra.execute(query))
    agrupamentos ={}
    contagens = {}
    for resultado in resultados:
        if resultado[0] != 'null':
           uf = resultado[0]
           if uf in agrupamentos: 
              agrupamentos[uf].append(1)
           else:
              agrupamentos[uf] = [1]
    
    
    for agrupamento in agrupamentos:
        contagens[agrupamento] = len(agrupamentos[agrupamento])

    return ordenarAgrupamento(contagens)[0:5]


def Q4C():
    medias = {}
    query = "SELECT *  FROM microdados WHERE NU_NOTA_REDACAO > -1 ALLOW FILTERING;"
    resultados = list(clienteCassandra.execute(query))
    resultadosFiltrados = []
    for resultado in resultados:
        ano = resultado[2]
        nota = resultado[1]
        if ano not in medias:
           query = "SELECT AVG(NU_NOTA_REDACAO) FROM microdados WHERE NU_ANO = "+str(ano)+" AND NU_NOTA_REDACAO > -1 ALLOW FILTERING;"  
           media = list(clienteCassandra.execute(query))
           medias[ano] = media[0][0]

        if nota < medias[ano]:
           resultadosFiltrados.append(resultado)


    return resultadosFiltrados


clienteCassandra = iniciarClienteCassandra()

def main ():

    resultados = Q1S()
    print('\n\n'+str(len(resultados))+' resultado(s) de Q1')

    resultados = Q2S()
    print('\n\n'+str(len(resultados))+' resultado(s) de Q2')

    resultados = Q3S()
    print('\n\n'+str(len(resultados))+' resultado(s) de Q3')

    resultados = Q4S()
    print('\n\n'+str(len(resultados))+' resultado(s) de Q4')

    resultados = Q1M()
    print(resultados)

    resultados = Q2M()
    print(resultados)

    resultados = Q3M()
    print(resultados)

    resultados = Q4M()
    print(resultados)

    resultados = Q1C()
    print(resultados)

    resultados = Q2C()
    print(resultados)


    resultados = Q3C()
    print(resultados)

    resultados = Q4C()
    print('\n\n'+str(len(resultados))+' resultado(s) de Q4C')

if __name__ == '__main__':
    main()