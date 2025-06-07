from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from netCDF4 import Dataset
import numpy as np
import os
import uuid
import time
import traceback

import httpx
from fastapi.responses import JSONResponse


app = FastAPI()



api_keys = [
    "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJjcGFjaGVjby5wZXJlbGxvQGdtYWlsLmNvbSIsImp0aSI6IjQ1NjgyODczLTczMTUtNGRkMS1hN2U2LTA3NzZlZThmNjY4MiIsImlzcyI6IkFFTUVUIiwiaWF0IjoxNzQ4MjkzNTU3LCJ1c2VySWQiOiI0NTY4Mjg3My03MzE1LTRkZDEtYTdlNi0wNzc2ZWU4ZjY2ODIiLCJyb2xlIjoiIn0.fvJNIic8vWZ7MpoPTwjRnvrnHvFi26wRHQ-uvciCuQE",
    "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJjcGFjaGVjby5wZXJlbGxvQGdtYWlsLmNvbSIsImp0aSI6IjQ2NjAxZjNkLTk3MGMtNGNiZS05ZTRjLTc3NmE3NzNiMTM4ZSIsImlzcyI6IkFFTUVUIiwiaWF0IjoxNzQ5MzA5NzIwLCJ1c2VySWQiOiI0NjYwMWYzZC05NzBjLTRjYmUtOWU0Yy03NzZhNzczYjEzOGUiLCJyb2xlIjoiIn0.u6YOgw5bXdb4eCW8jubn21ssWc84X-urDpqwsX-4HsA",
    "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJjcGFjaGVjby5wZXJlbGxvQGdtYWlsLmNvbSIsImp0aSI6IjczYjI0OGQ3LTdjOTktNDc5Ni04NjA3LTM1MzNmYmYyM2QzMSIsImlzcyI6IkFFTUVUIiwiaWF0IjoxNzQ5MzEwNTYzLCJ1c2VySWQiOiI3M2IyNDhkNy03Yzk5LTQ3OTYtODYwNy0zNTMzZmJmMjNkMzEiLCJyb2xlIjoiIn0.eIZZqjV9HqVkbak6YDjIh_Z0pTxxSwQcimS7fWiUwRw"
]

max_retries = 3
base_wait_time_ms = 8000  # tiempo base en milisegundos



app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def create_variable(group, name, values, dim_name):
    force_as_string = {"fecha", "fecha_inicio", "fecha_fin"}  # Otros campos si quieres

    if name in force_as_string or any(isinstance(v, str) for v in values):
        # Guardar como variable string nativa (tipo variable Unicode)
        dt = str  # netCDF4 entiende que es string unicode

        var = group.createVariable(name, dt, (dim_name,))
        var[:] = np.array(values, dtype=object)
    else:
        # Guardar como float
        arr = np.array(values, dtype=np.float32)
        var = group.createVariable(name, np.float32, (dim_name,))
        var[:] = arr
@app.get("/ping")
async def ping():
    return {"status": "alive"}

@app.post("/generate-netcdf/")
async def generate_netcdf(request: Request):
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON inválido")

    tmp_filename = f"{uuid.uuid4()}.nc"

    try:
        ds = Dataset(tmp_filename, "w", format="NETCDF4")

        # Crear grupo para Datos
        datos = data.get("Datos", [])
        n_datos = len(datos)
        datos_grp = ds.createGroup("datos")
        datos_grp.createDimension("registro", n_datos)
        if n_datos > 0:
            for key in datos[0].keys():
                valores = [d.get(key, "") if d.get(key) is not None else "" for d in datos]
                create_variable(datos_grp, key, valores, "registro")

        # Crear grupo para Estaciones
        estaciones = data.get("Estaciones", [])
        n_est = len(estaciones)
        estaciones_grp = ds.createGroup("estaciones")
        estaciones_grp.createDimension("estacion", n_est)
        if n_est > 0:
            for key in estaciones[0].keys():
                valores = [e.get(key, "") if e.get(key) is not None else "" for e in estaciones]
                create_variable(estaciones_grp, key, valores, "estacion")

        # Guardar Metadatos como atributos globales
        metadatos = data.get("Metadatos", {})
        if isinstance(metadatos, dict):
            for key, val in metadatos.items():
                ds.setncattr(key, str(val))
        elif isinstance(metadatos, list):
            for i, campo in enumerate(metadatos):
                for key, val in campo.items():
                    ds.setncattr(f"{key}_{i}", str(val))

        ds.title = "Datos exportados desde JSON a NetCDF"
        ds.close()

        file_stream = open(tmp_filename, "rb")
        response = StreamingResponse(file_stream, media_type="application/netcdf")
        response.headers["Content-Disposition"] = f"attachment; filename=datos_{uuid.uuid4()}.nc"
        return response

    except Exception as e:
        tb_str = traceback.format_exc()
        print(tb_str)  # Imprime el error completo en logs
        raise HTTPException(status_code=500, detail=f"Error creando NetCDF: {e}")

    finally:
        time.sleep(1)
        if os.path.exists(tmp_filename):
            os.remove(tmp_filename)




@app.get("/fetch-data-retry/")
async def fetch_with_retries(url: str, tipo: str):
    async with httpx.AsyncClient() as client:
        # Intentamos por cada API key
        for api_key in api_keys:
            endpoint_with_api_key = url.replace("{apiKey}", api_key)

            for attempt in range(max_retries):
                try:
                    # Hacemos la solicitud con la API key actual
                    response = await client.get(endpoint_with_api_key)

                    if response.status_code != 200:
                        raise HTTPException(status_code=response.status_code, detail=f"Error HTTP {response.status_code}")

                    data = response.json()  # Parseamos la respuesta a JSON
                    return JSONResponse(content=data)

                except httpx.RequestError as e:
                    # Si falla la solicitud, se captura el error y se hace un reintento
                    if attempt < max_retries - 1:
                        wait_time = base_wait_time_ms * (2 ** attempt) / 1000  # Exponential backoff
                        time.sleep(wait_time)  # Esperamos antes de hacer el siguiente intento
                        continue
                    else:
                        return JSONResponse(
                            content={"error": f"Failed after {max_retries} attempts: {str(e)}"},
                            status_code=500
                        )
                except Exception as e:
                    # Cualquier otro tipo de error
                    return JSONResponse(
                        content={"error": f"Error al procesar la solicitud: {str(e)}"},
                        status_code=500
                    )

    return JSONResponse(content={"error": "No valid API key or max retries exceeded"}, status_code=500)