#!/usr/bin/python
# -*- coding: utf-8 -*-


import redis
from memory_profiler import profile


class ControladorRedis:


    def __init__(self):
        global clienteRedis
        clienteRedis = self.iniciarClienteRedis() 


    def iniciarClienteRedis(self):
        r = redis.Redis()
        r.flushdb()
        return r
   

    @profile
    def ordenarRegistros(self,resultados,campo):
        
        lista = []

        for resultado in resultados:
            if resultados[resultado][campo] != 'null':
               lista.append(resultados[resultado][campo]);
        

        resultadosOrdenados = sorted(lista, reverse=True)

        return resultadosOrdenados


    @profile   
    def ordenarAgrupamento(self,resultados):
        resultadosOrdenados =sorted(resultados.items(), key=lambda x:x[1], reverse=True)
        return resultadosOrdenados


    @profile
    def retornarChaves(self,expressaoRegular,limit):
        resultado = {}
        for key in clienteRedis.scan_iter(expressaoRegular):
            resultado[key] = clienteRedis.hgetall(key)
            if limit > 0 and len(resultado) >= limit:
               break
      
        return resultado


    @profile
    def mediaRedacaoAgrupadaPorRaca(self,resultados):
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

    
    @profile
    def mediaMatematicaAgrupadaPorAno(self,resultados):
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


    @profile
    def contagemEvasaoGeralAgrupadaPorUf(self,resultados):
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
     

    @profile       
    def mediaRedacaoAgrupadaPorAno(self,resultados):
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
    

    @profile
    def mergeResultados(self,resultado, dictsParaMerge):
        for dic in dictsParaMerge:
            resultado.update(dic)
        return resultado


    @profile
    def Q1S(self):
       return self.retornarChaves('*',0)

    
    @profile
    def Q2S(self):
        minorias = ['Preta', 'Indigena', 'Amarela', 'Parda']

        resultados={}
        for minoria in minorias:
            retorno = self.retornarChaves("*TP_COR_RACA:"+minoria+"*",0)
            resultados.update(retorno)

        return resultados


    @profile
    def Q3S(self):
        return self.retornarChaves("*IN_DEFICIENCIA_*:1*-TP_PRESENCA_CN*",0)

    
    @profile
    def Q4S(self):
        return self.retornarChaves("*TP_NACIONALIDADE:Brasileiro*",0)


    @profile
    def Q1M(self):
        resultados = self.retornarChaves("*NU_ANO:2019*",0)
        resultadosOrdenados = self.ordenarRegistros(resultados,"NU_NOTA_REDACAO")
        return resultadosOrdenados[0:5]


    @profile
    def Q2M(self):
        resultados = self.Q2S()
        resultadosOrdenados = self.ordenarRegistros(resultados,"NU_NOTA_REDACAO")
        return resultadosOrdenados[0:5]
    

    @profile
    def Q3M(self):
        resultados = self.retornarChaves("*NU_IDADE:[1][5-8]*",0)
        resultadosOrdenados = self.ordenarRegistros(resultados,"NU_NOTA_REDACAO")
        return resultadosOrdenados[0:5]

    
    @profile
    def Q4M(self):
        resultados = self.retornarChaves("*",0)
        contagem = 0
        for resultado in resultados:
          gabarito = resultados[resultado]['TX_GABARITO_LC']
          respostas = resultados[resultado]['TX_RESPOSTAS_LC']
          if respostas !='null' and gabarito[0:5] == respostas[0:5]:
             contagem = contagem+1
        return contagem
    

    @profile    
    def Q1C(self):
         resultados = self.retornarChaves("*NU_ANO:2018*",0)
         agrupamento = self.mediaRedacaoAgrupadaPorRaca(resultados)
         agrupamentoOrdenado = self.ordenarAgrupamento(agrupamento)
         return agrupamentoOrdenado


    @profile
    def Q2C(self):
         resultados = self.retornarChaves("*Q005:[5-9]*",0)
         agrupamento = self.mediaMatematicaAgrupadaPorAno(resultados)
         agrupamentoOrdenado = self.ordenarAgrupamento(agrupamento)
         return agrupamentoOrdenado


    @profile
    def Q3C(self):
        resultados = self.retornarChaves("*0-TP_PRESENCA_CN:0*TP_PRESENCA_CH:0*TP_PRESENCA_LC:0*TP_PRESENCA_MT:0*",0)
        agrupamento = self.contagemEvasaoGeralAgrupadaPorUf(resultados)
        agrupamentoOrdenado = self.ordenarAgrupamento(agrupamento)
        return agrupamentoOrdenado[0:5]


    @profile
    def Q4C(self):
        inscritosAbaixoMedia = []
        resultados = self.retornarChaves("**",0)
        agrupamento = self.mediaRedacaoAgrupadaPorAno(resultados)
        for resultado in resultados:
        	if resultados[resultado]['NU_NOTA_REDACAO'] != 'null':
        		ano = resultados[resultado]['NU_ANO']
        		nota = float(resultados[resultado]['NU_NOTA_REDACAO'])
        		mediaAno = agrupamento[ano]
        		if nota < mediaAno:
        		   inscritosAbaixoMedia.append(resultados[resultado])

        return inscritosAbaixoMedia