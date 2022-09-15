from fastapi import FastAPI, UploadFile, File, Form
from databases import Database


app = FastAPI()

database = Database("sqlite:///Racedb.db")


@app.on_event("startup")
async def database_connect():
    await database.connect()


@app.on_event("shutdown")
async def database_disconnect():
    await database.disconnect()


@app.get("/Q1 Año con mas carreras")
async def fetch_data(Cantidad_de_Resultados: int):
    
    Consulta = "SELECT Year, sum(Round) as Carreras_totales FROM Races GROUP BY Year ORDER BY Carreras_Totales DESC LIMIT {};".format(str(Cantidad_de_Resultados))
    results = await database.fetch_all(query=Consulta)
    return results

@app.get("/Q2 Piloto con mayor cantidad de primeros puestos")
async def fetch_data(Cantidad_de_Resultados: int):
    
    Consulta = "SELECT D.DriverRef as NombrePiloto, count(PositionOrder) as Veces1erPuesto FROM Results R JOIN Drivers D ON (R.DriverId = D.DriverId) WHERE PositionOrder = 1 GROUP BY D.DriverId ORDER BY Veces1erPuesto DESC LIMIT {};".format(str(Cantidad_de_Resultados))
    results = await database.fetch_all(query=Consulta)
    return results


@app.get("/Q3 Piloto con mayor cantidad de primeros puestos")
async def fetch_data(Cantidad_de_Resultados: int):
    
    Consulta = "SELECT C.Name as CName, count(C.CircuitId) As TimesRide FROM Races R JOIN Circuits C ON (R.CircuitId = C.CircuitId) GROUP BY C.CircuitId ORDER BY TimesRide DESC LIMIT {};".format(str(Cantidad_de_Resultados))
    results = await database.fetch_all(query=Consulta)
    return results


@app.get("/Q4 Piloto con más puntos en total(America/British)")
async def fetch_data(Cantidad_de_Resultados: int):
    
    Consulta = "SELECT sum(R.Points), R.DriverId, D.Surname, D.Forename FROM Results R JOIN Drivers D ON (R.DriverId = D.DriverId) JOIN Constructors C ON (C.ConstructorId = R.ConstructorId) WHERE C.Nationality = 'British' OR C.Nationality = 'American' GROUP BY R.DriverId ORDER BY SUM(points) DESC LIMIT {};".format(str(Cantidad_de_Resultados))
    results = await database.fetch_all(query=Consulta)
    return results


@app.get("/GET para hacer consultas SQL")
async def fetch_data(ConsultaSQL: str):
    
    Consulta = "{};".format(str(ConsultaSQL))
    results = await database.fetch_all(query=Consulta)
    return results
