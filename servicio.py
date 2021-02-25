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
    #print(anio)
    df = pd.read_csv('./source/consulta1_.csv/part-00000-a861331f-df98-4c54-9be6-c45f446720cf-c000.csv', header=0 , sep=',')
    df = df.sort_values('Year')
    #print(df)
    df.set_index('Year' , inplace=True)
    #print(df)
    print(df.loc[anio , ['Origin', 'Dest' , 'count']])
    df1 = df.loc[anio , ['Origin', 'Dest' , 'count']]
    datos = []
    for registro in df1.itertuples():
        dicDatos = {'origen' : registro.Origin , 'destino' : registro.Dest , 'retraso': registro.count}
        datos.append(dicDatos)
        dicdatos = {}
    dicJson = {anio : datos}
    return json.dumps(dicJson)

@app.route('/consulta2' , methods = ['GET'])
def consulta2():
    #anio = '1987'
    anio = request.args.get('anio')
    df = pd.read_csv('./source/consulta2_.csv/part-00000-0d685a34-50ce-44b6-82a2-c70f3869bdba-c000.csv', header=0 , sep=',')
    df = df.sort_values('Year')
    df.set_index('Year' , inplace=True)
    df1 = df.loc[anio , ['UniqueCarrier', 'count']]
    print(df1)
    datos = []
    for registro in df1.itertuples():
        dicDatos = {'aerolinea' : registro.UniqueCarrier , 'retraso' : registro.count}
        datos.append(dicDatos)
        dicdatos = {}
    dicJson = {anio : datos}
    return json.dumps(dicJson)


@app.route('/consulta4' , methods = ['GET'])
def consulta4():
    #anio = '1987'
    anio = request.args.get('anio')
    df = pd.read_csv('./source/consulta4_.csv/part-00000-8e04c36e-aa83-40d0-b8d7-fb17318ffbec-c000.csv', header=0 , sep=',')
    df = df.sort_values('Year')
    df.set_index('Year' , inplace=True)
    df1 = df.loc[anio , ['Month', 'UniqueCarrier' , 'count']]
    datos = []
    for registro in df1.itertuples():
        dicDatos = {'mes' : registro.Month , 'aerolinea' : registro.UniqueCarrier , 'retrasos' : registro.count}
        datos.append(dicDatos)
        dicdatos = {}
    dicJson = {anio : datos}
    return json.dumps(dicJson)

@app.route('/consulta5' , methods = ['GET'])
def consulta5():
    #anio = '1987'
    anio = request.args.get('anio')
    df = pd.read_csv('./source/consulta5_.csv/part-00000-b9329e8a-0722-48ad-abe8-ca47bb48619e-c000.csv', header=0 , sep=',')
    df = df.sort_values('Year')
    df.set_index('Year' , inplace=True)
    df1 = df.loc[anio , [ 'UniqueCarrier', 'Origin' , 'Dest' , 'count']]
    datos = []
    for registro in df1.itertuples():
        dicDatos = {'origen': registro.Origin, 'destino':registro.Dest , 'aerolinea':registro.UniqueCarrier , 'vuelos' : registro.count}
        datos.append(dicDatos)
        dicdatos = {}
    dicJson = {anio : datos}
    return json.dumps(dicJson)


if __name__ == '__main__':
    app.run( host = '0.0.0.0', port = 2020, debug = True )