from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import json
from netCDF4 import Dataset
import numpy as np
import os
import uuid
import io
import time

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_methods=["*"],
    allow_headers=["*"],
)

def create_variable(ds, name, values, dim_name):
    # Intentamos inferir tipo de variable numérica o string
    try:
        arr = np.array(values, dtype=np.float32)
        var = ds.createVariable(name, np.float32, (dim_name,))
        var[:] = arr
    except Exception:
        # Guardamos strings (como arrays de caracteres)
        max_len = max(len(str(v)) for v in values)
        ds.createDimension(f"{name}_str_len", max_len)
        var = ds.createVariable(name, 'S1', (dim_name, f"{name}_str_len"))
        # Convertir strings a array de caracteres ASCII
        arr = np.array([list(str(v).ljust(max_len)) for v in values], dtype='S1')
        var[:, :] = arr

@app.post("/generate-netcdf/")
async def generate_netcdf(file: UploadFile = File(...)):
    try:
        data_bytes = await file.read()
        data = json.loads(data_bytes)
    except Exception:
        raise HTTPException(status_code=400, detail="JSON inválido")

    tmp_filename = f"{uuid.uuid4()}.nc"

    try:
        ds = Dataset(tmp_filename, "w", format="NETCDF4")

        # --- DATOS ---
        datos = data.get("Datos", [])
        n_datos = len(datos)
        ds.createDimension("registro", n_datos)

        if n_datos > 0:
            # Para cada campo en el primer objeto de datos, creamos variable
            for key in datos[0].keys():
                valores = [d.get(key, "") for d in datos]
                create_variable(ds, key, valores, "registro")

        # --- ESTACIONES ---
        estaciones = data.get("Estaciones", [])
        n_est = len(estaciones)
        ds.createDimension("estacion", n_est)

        if n_est > 0:
            for key in estaciones[0].keys():
                valores = [e.get(key, "") for e in estaciones]
                create_variable(ds, key, valores, "estacion")

        # --- METADATOS ---
        # Suponemos que metadatos es dict o tiene campo 'campos'
        metadatos = data.get("Metadatos", {})
        if isinstance(metadatos, dict):
            for key, val in metadatos.items():
                # Guardamos como atributo global, convirtiendo a str
                ds.setncattr(key, str(val))
        elif isinstance(metadatos, list) and len(metadatos) > 0:
            # Si es lista de campos
            for i, campo in enumerate(metadatos):
                for key, val in campo.items():
                    ds.setncattr(f"{key}_{i}", str(val))

        ds.title = "Datos exportados desde FastAPI"

        ds.close()

        file_stream = open(tmp_filename, "rb")
        response = StreamingResponse(file_stream, media_type="application/netcdf")
        response.headers["Content-Disposition"] = "attachment; filename=datos.nc"
        return response

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creando NetCDF: {e}")

    finally:
        time.sleep(1)
        if os.path.exists(tmp_filename):
            os.remove(tmp_filename)
