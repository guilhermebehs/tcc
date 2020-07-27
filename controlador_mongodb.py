import pymongo
import re
from memory_profiler import profile



class ControladorMongoDB:


    def iniciarClienteMongoDB (self):
        clienteMongo = pymongo.MongoClient("mongodb://localhost:27017/")
        baseEstudo = clienteMongo["tcc"]
        colecaoMicrodados = baseEstudo["microdados"]
        return colecaoMicrodados

    def __init__(self):
        global microdados
        microdados = self.iniciarClienteMongoDB() 

    @profile
    def Q1S(self):
       return microdados.find({}).count()

    @profile
    def Q2S(self):
	   return microdados.find({'TP_COR_RACA':{"$nin":['Branca','Nao declarado']}}).count()
    
    @profile
    def Q3S(self):
	   return microdados.find({"$or":[{'IN_DEFICIENCIA_FISICA':True},{'IN_DEFICIENCIA_MENTAL':True}]}).count()

    @profile
    def Q4S(self):	
	   rgx = re.compile('.*Brasileiro.*', re.IGNORECASE)	
	   return microdados.find({"TP_NACIONALIDADE": rgx}).count()
    
    @profile
    def Q1M(self):   
	   return list(microdados.find({"$and":[{'NU_ANO':2019},{'NU_NOTA_REDACAO':{"$ne":None}}]},{'_id':0,'NU_NOTA_REDACAO':1}).sort([('NU_NOTA_REDACAO',-1)]).limit(5))
    
    @profile
    def Q2M(self):
	   return list(microdados.find({'TP_COR_RACA':{"$not":{"$in":['Branca','Nao declarado']}}},{'_id':0,'NU_NOTA_REDACAO':1}).sort([('NU_NOTA_REDACAO',-1)]).limit(5))
    
    @profile
    def Q3M(self):
	   return list(microdados.find({'NU_IDADE':{"$gte":15,"$lte":18}},{'_id':0,'NU_NOTA_REDACAO':1}).sort([('NU_NOTA_REDACAO',-1)]).limit(5))
    
    @profile
    def Q4M(self):
	   return microdados.find({"$and":[{'TX_RESPOSTAS_LC':{"$ne":None}},{"$where":'this.TX_RESPOSTAS_LC.substring(0,5) == this.TX_GABARITO_LC.substring(0,5)'}]}).count()

    @profile
    def Q1C(self):
	  return list(microdados.aggregate([
	                         {'$match' : { 'NU_ANO' : 2018, 'NU_NOTA_REDACAO':{'$ne':None} } },
	                         {
	                         '$group':{
	                                 '_id':{'raca':'$TP_COR_RACA'}, 
	                                 'media':{'$avg':'$NU_NOTA_REDACAO'}
	                                 }},
	                         {'$sort':{'media':-1}}
	                         
	                                 ]))

    @profile
    def Q2C(self):
	  return list(microdados.aggregate([
	                         {'$match' : { 'Q005':{'$gte':5,'$lte':9} }},
	                         {
	                         '$group':{
	                                 '_id':{'ano':'$NU_ANO'}, 
	                                 'media':{'$avg':'$NU_NOTA_MT'}
	                                 }},
	                         {'$sort':{'media':-1}}
	                    ]))
    @profile
    def Q3C(self):
	  return list(microdados.aggregate([
	                         {'$match' : {'$and':[{ 'TP_PRESENCA_CH' : '0', 'TP_PRESENCA_CN' : '0', 'TP_PRESENCA_LC':'0', 'TP_PRESENCA_MT':'0' }]} },
	                         {
	                         '$group':{
	                                 '_id':{'Estado':'$SG_UF_RESIDENCIA'}, 
	                                 'contagem':{'$sum': 1}
	                                 }},
	                         {'$sort':{'contagem':-1}},
	                         {'$limit':5}
	   ]))
    
    @profile
    def Q4C(self): 
	 
	 inscritos = []

	 listaMedias = list(microdados.aggregate([
	                         {'$match' : {'NU_NOTA_REDACAO':{'$ne':None} } },
	                         {
	                         '$group':{
	                                 '_id':{'ano':'$NU_ANO'}, 
	                                 'media':{'$avg':'$NU_NOTA_REDACAO'}
	                                 }},
	                         {'$sort':{'media':-1}}
	                         
	                                 ]))
	 andClausura = []
	 for media in listaMedias:
	     ano = media['_id']['ano']
	     media = media['media']
	     andClausura.append({'$and':[{'NU_NOTA_REDACAO':{'$lte':media}},{'NU_ANO':ano}]})

	 inscritos = list(microdados.find({'$or':andClausura}))

	 return len(inscritos)
	  