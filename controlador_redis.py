#!/usr/bin/python
# -*- coding: utf-8 -*-


import redis




def iniciarClienteRedis():
    r = redis.Redis()
    return r

def ordenarRegistros(resultados,campo):
    
    dicionario = {}

    for resultado in resultados:
        if resultados[resultado][campo] != 'null':
           dicionario[resultado] = resultados[resultado][campo];

    resultadosOrdenados =sorted(dicionario.items(), key=lambda x:x[1], reverse=True)
    resultadosOrdenados = resultadosOrdenados[0:5]

    return resultadosOrdenados[0:5]
   
def ordenarAgrupamento(resultados):
    resultadosOrdenados =sorted(resultados.items(), key=lambda x:x[1], reverse=True)
    return resultadosOrdenados     

def retornarChaves(expressaoRegular,limit):
    resultado = {}
    for key in clienteRedis.scan_iter(expressaoRegular):
        resultado[key] = clienteRedis.hgetall(key)
        if limit > 0 and len(resultado) >= limit:
           break
  
    return resultado

def mediaRedacaoAgrupadaPorRaca(resultados):
    agrupamentos ={}
    medias = {}
    for resultado in resultados:
        if resultados[resultado]['NU_NOTA_REDACAO'] != 'null':
           raca = resultados[resultado]['TP_COR_RACA']
           nota = resultados[resultado]['NU_NOTA_REDACAO']
           if raca in agrupamentos: 
              agrupamentos[raca].append(float(nota))
           else:
              agrupamentos[raca] = [float(nota)]
    
    
    for agrupamento in agrupamentos:
         media = sum(agrupamentos[agrupamento]) / len(agrupamentos[agrupamento])
         medias[agrupamento] = media
          
    return medias


def mediaMatematicaAgrupadaPorAno(resultados):
    agrupamentos ={}
    medias = {}
    for resultado in resultados:
        if resultados[resultado]['NU_NOTA_MT'] != 'null':
           ano = resultados[resultado]['NU_ANO']
           nota = resultados[resultado]['NU_NOTA_MT']
           if ano in agrupamentos: 
              agrupamentos[ano].append(float(nota))
           else:
              agrupamentos[ano] = [float(nota)]
    
    
    for agrupamento in agrupamentos:
         media = sum(agrupamentos[agrupamento]) / len(agrupamentos[agrupamento])
         medias[agrupamento] = media
          
    return medias

def contagemEvasaoGeralAgrupadaPorUf(resultados):
    agrupamentos ={}
    contagens = {}
    for resultado in resultados:
         uf = resultados[resultado]['SG_UF_RESIDENCIA']
         if uf in agrupamentos: 
            agrupamentos[uf].append(1)
         else:
            agrupamentos[uf] = [1]
    
    
    for agrupamento in agrupamentos:
         soma = sum(agrupamentos[agrupamento]) 
         contagens[agrupamento] = soma
          
    return contagens
        
def mediaRedacaoAgrupadaPorAno(resultados):
    agrupamentos ={}
    contagemMedias = {}

    for resultado in resultados:
    	if resultados[resultado]['NU_NOTA_REDACAO'] != 'null':
         ano = resultados[resultado]['NU_ANO']
         nota = resultados[resultado]['NU_NOTA_REDACAO']
         if ano in agrupamentos: 
            agrupamentos[ano].append(float(nota))
         else:
            agrupamentos[ano] = [float(nota)]
    
    
    for agrupamento in agrupamentos:
         media = sum(agrupamentos[agrupamento])/len(agrupamentos[agrupamento])
         contagemMedias[agrupamento] =  media
          
    return contagemMedias

def mergeResultados(resultado, dictsParaMerge):
    for dic in dictsParaMerge:
        resultado.update(dic)
    return resultado

def Q1S():
   return retornarChaves('*',1)

def Q2S():
    minorias = ['Preta', 'Indigena', 'Amarela', 'Parda']

    resultados={}
    for minoria in minorias:
        retorno = retornarChaves("*TP_COR_RACA:"+minoria+"*",0)
        resultados.update(retorno)

    return resultados


def Q3S():
    return retornarChaves("*IN_DEFICIENCIA_*:1*-TP_PRESENCA_CN*",0)

def Q4S():
    return retornarChaves("*TP_NACIONALIDADE:Brasileiro*",0)

def Q1M():
    resultados = retornarChaves("*NU_ANO:2019*",0)
    resultadosOrdenados = ordenarRegistros(resultados,"NU_NOTA_REDACAO")
    return resultadosOrdenados

def Q2M():
    resultados = Q2S()
    resultadosOrdenados = ordenarRegistros(resultados,"NU_NOTA_REDACAO")
    return resultadosOrdenados

def Q3M():
    resultados = retornarChaves("*NU_IDADE:[1][5-8]*",0)
    resultadosOrdenados = ordenarRegistros(resultados,"NU_NOTA_REDACAO")
    return resultadosOrdenados

def Q4M():
    resultados = retornarChaves("*",0)
    contagem = 0
    for resultado in resultados:
      gabarito = resultados[resultado]['TX_GABARITO_LC']
      respostas = resultados[resultado]['TX_RESPOSTAS_LC']
      if respostas !='null' and gabarito[0:5] == respostas[0:5]:
         contagem = contagem+1
    return contagem
    
def Q1C():
     resultados = retornarChaves("*NU_ANO:2018*",0)
     agrupamento = mediaRedacaoAgrupadaPorRaca(resultados)
     agrupamentoOrdenado = ordenarAgrupamento(agrupamento)
     return agrupamentoOrdenado

def Q2C():
     resultados = retornarChaves("*Q005:[5-9]*",0)
     agrupamento = mediaMatematicaAgrupadaPorAno(resultados)
     agrupamentoOrdenado = ordenarAgrupamento(agrupamento)
     return agrupamentoOrdenado


def Q3C():
    resultados = retornarChaves("*0-TP_PRESENCA_CN:0*TP_PRESENCA_CH:0*TP_PRESENCA_LC:0*TP_PRESENCA_MT:0*",0)
    agrupamento = contagemEvasaoGeralAgrupadaPorUf(resultados)
    agrupamentoOrdenado = ordenarAgrupamento(agrupamento)
    return agrupamentoOrdenado[0:5]

def Q4C():
    inscritosAbaixoMedia = []
    resultados = retornarChaves("**",0)
    agrupamento = mediaRedacaoAgrupadaPorAno(resultados)
    for resultado in resultados:
    	if resultados[resultado]['NU_NOTA_REDACAO'] != 'null':
    		ano = resultados[resultado]['NU_ANO']
    		nota = float(resultados[resultado]['NU_NOTA_REDACAO'])
    		mediaAno = agrupamento[ano]
    		if nota < mediaAno:
    		   inscritosAbaixoMedia.append(resultados[resultado])

    return inscritosAbaixoMedia

clienteRedis = iniciarClienteRedis()

def main ():

  #
    resultados = Q4C()
    print(resultados)
  #  for key in resultados:
  #      print(key)
    print('\n\n'+str(len(resultados))+' resultado(s)')



if __name__ == '__main__':
    main()