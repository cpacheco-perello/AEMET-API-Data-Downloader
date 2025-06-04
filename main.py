from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import json
from netCDF4 import Dataset
import numpy as np
import os
import uuid
import time
from fastapi import Request
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_methods=["*"],
    allow_headers=["*"],
)

def create_variable(ds, name, values, dim_name):
    # Intentar crear variable numérica float
    try:
        arr = np.array(values, dtype=np.float32)
        var = ds.createVariable(name, np.float32, (dim_name,))
        var[:] = arr
    except Exception:
        # Si falla, crear variable string (S1)
        max_len = max(len(str(v)) for v in values)
        ds.createDimension(f"{name}_str_len", max_len)
        var = ds.createVariable(name, 'S1', (dim_name, f"{name}_str_len"))
        arr = np.array([list(str(v).ljust(max_len)) for v in values], dtype='S1')
        var[:, :] = arr

import traceback

@app.post("/generate-netcdf/")
async def generate_netcdf(request: Request):
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON inválido")


    tmp_filename = f"{uuid.uuid4()}.nc"

    try:
        ds = Dataset(tmp_filename, "w", format="NETCDF4")

        # Crear dimensión y variables para Datos
        datos = data.get("Datos", [])
        n_datos = len(datos)
        ds.createDimension("registro", n_datos)
        if n_datos > 0:
            for key in datos[0].keys():
                valores = [d.get(key, "") if d.get(key) is not None else "" for d in datos]
                create_variable(ds, key, valores, "registro")

        # Crear dimensión y variables para Estaciones
        estaciones = data.get("Estaciones", [])
        n_est = len(estaciones)
        ds.createDimension("estacion", n_est)
        if n_est > 0:
            for key in estaciones[0].keys():
                valores = [e.get(key, "") if e.get(key) is not None else "" for e in estaciones]
                create_variable(ds, key, valores, "estacion")

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
        print(tb_str)  # Esto imprimirá el error completo en logs
        raise HTTPException(status_code=500, detail=f"Error creando NetCDF: {e}")

    finally:
        time.sleep(1)
        if os.path.exists(tmp_filename):
            os.remove(tmp_filename)