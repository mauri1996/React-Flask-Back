from flask import *
import pandas as pd
import json
from flask_cors import CORS, cross_origin

app = Flask(__name__)
CORS(app)
@app.route('/consulta1' , methods = ['GET'])
def consulta1():
    #anio = '1987'
    #anio = request.form['anio_consult1a']
	anio = request.args.get('anio')
	df = pd.read_csv('./source/consulta1_.csv/resultado1.csv', header=0 , sep=',')
	df = df.sort_values('Year')
	df.set_index('Year' , inplace=True)
	try:
		df1 = df.loc[anio , ['Origin', 'Dest' , 'count']]
		datos = []
		for registro in df1.itertuples():
			dicDatos = {'origen' : registro.Origin , 'destino' : registro.Dest , 'retraso': registro.count}
			datos.append(dicDatos)
			dicdatos = {}
		dicJson = {anio : datos}
	except:
		dicDatos = {'origen' : 'No Data' , 'retraso' : 'No data','destino' : 'No Data' ,}
		datos.append(dicDatos)
		dicJson = {anio : datos}
	return json.dumps(dicJson)

@app.route('/consulta2' , methods = ['GET'])
def consulta2():
	anio = request.args.get('anio')
	df = pd.read_csv('./source/consulta2_.csv/resultado2.csv', header=0 , sep=',')
	df = df.sort_values('Year')
	df.set_index('Year' , inplace=True)
	datos = []
	try:
		df1 = df.loc[anio , ['UniqueCarrier', 'count']]		
		if (isinstance(df1, pd.core.series.Series)):
			#print(type(df1.count()))
			df1 = df1.to_frame().T
			df1 =df1.rename(columns={'count': 'retrasos'})
			dicDatos = {'aerolinea' : df1.iloc[0].UniqueCarrier, 'retraso' : df1.iloc[0].retrasos.astype('str')}
			datos.append(dicDatos)
			dicJson = {anio : datos}
		else:
			#print ('no es serie')		
			for registro in df1.itertuples():        
				dicDatos = {'aerolinea' : registro.UniqueCarrier , 'retraso' : registro.count}
				datos.append(dicDatos)
				dicdatos = {}
			dicJson = {anio : datos}
	except:
		dicDatos = {'aerolinea' : 'No Data' , 'retraso' : 'No data'}
		datos.append(dicDatos)
		dicJson = {anio : datos}		

	return json.dumps(dicJson)

@app.route('/consulta3' , methods = ['GET'])
def consulta3():
	anio = request.args.get('anio')
	df = pd.read_csv('./source/consulta3_.csv/resultado3.csv', header=0 , sep=',')
	df = df.sort_values('Year')
	df.set_index('Year' , inplace=True)
	datos=[]
	try:
		df1 = df.loc[int(anio) , ['UniqueCarrier', 'count']]
		if (isinstance(df1, pd.core.series.Series)):
			df1 = df1.to_frame().T
			df1 =df1.rename(columns={'count': 'retrasos'})
			print(df1.iloc[0].retrasos)
			dicDatos = {'aerolinea' : df1.iloc[0].UniqueCarrier, 'retraso' : df1.iloc[0].retrasos.astype('str')}
			datos.append(dicDatos)
			dicJson = {anio : datos}
		else:            
			for registro in df1.itertuples():
				dicDatos = {'aerolinea' : registro.UniqueCarrier , 'retraso' : registro.count}
				datos.append(dicDatos)
				dicdatos = {}
			dicJson = {anio : datos}
	except:
		dicDatos = {'aerolinea' : 'No Data' , 'retraso' : 'No data'}
		datos.append(dicDatos)
		dicJson = {anio : datos}

	return json.dumps(dicJson)

@app.route('/consulta4' , methods = ['GET'])
def consulta4():
    #anio = '1987'
	anio = request.args.get('anio')
	df = pd.read_csv('./source/consulta4_.csv/resultado4.csv', header=0 , sep=',')
	df = df.sort_values('Year')
	df.set_index('Year' , inplace=True)	
	datos = []
	try:
		df1 = df.loc[anio , ['Month', 'UniqueCarrier' , 'count']]
		if (isinstance(df1, pd.core.series.Series)):
			#print(type(df1.count()))
			df1 = df1.to_frame().T
			df1 =df1.rename(columns={'count': 'retrasos'}) 
			dicDatos = {'mes' : df1.iloc[0].Month.astype('str') , 'aerolinea' : df1.iloc[0].UniqueCarrier , 'retrasos' : df1.iloc[0].retrasos.astype('str')}
			datos.append(dicDatos)
			dicJson = {anio : datos}	
		else:
			for registro in df1.itertuples():
				dicDatos = {'mes' : registro.Month , 'aerolinea' : registro.UniqueCarrier , 'retrasos' : registro.count}
				datos.append(dicDatos)
				dicdatos = {}
			dicJson = {anio : datos}
	except:
		dicDatos = {'aerolinea' : 'No Data' , 'retrasos' : 'No data', 'mes' : 'No data'}
		datos.append(dicDatos)
		dicJson = {anio : datos}
	return json.dumps(dicJson)


@app.route('/consulta5' , methods = ['GET'])
def consulta5():
	anio = request.args.get('anio')
	df = pd.read_csv('./source/consulta5_.csv/resultado5.csv', header=0 , sep=',')
	df = df.sort_values('Year')
	df.set_index('Year' , inplace=True)	
	datos = []
	try:
		df1 = df.loc[anio , [ 'UniqueCarrier', 'Origin' , 'Dest' , 'count']]
		if (isinstance(df1, pd.core.series.Series)):
			#print(type(df1.count()))
			df1 = df1.to_frame().T
			df1 =df1.rename(columns={'count': 'retrasos'})
			dicDatos = {'origen': df1.iloc[0].Origin, 'destino': df1.iloc[0].Dest , 'aerolinea': df1.iloc[0].UniqueCarrier , 'vuelos' : df1.iloc[0].retrasos.astype('str')}
			datos.append(dicDatos)
			dicJson = {anio : datos}
		else:
			for registro in df1.itertuples():
				dicDatos = {'origen': registro.Origin, 'destino':registro.Dest , 'aerolinea':registro.UniqueCarrier , 'vuelos' : registro.count}
				datos.append(dicDatos)
				dicdatos = {}
			dicJson = {anio : datos}
	except:
		dicDatos = {'aerolinea' : 'No Data' , 'vuelos' : 'No data', 'origen' : 'No data', 'destino' : 'No data'}
		datos.append(dicDatos)
		dicJson = {anio : datos}

	return json.dumps(dicJson)


if __name__ == '__main__':
    app.run( host = '0.0.0.0', port = 2020, debug = True )