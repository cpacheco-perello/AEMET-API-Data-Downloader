from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from netCDF4 import Dataset
import numpy as np
import os
import uuid
import time
import traceback

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def create_variable(group, name, values, dim_name):
    force_as_string = {"fecha", "fecha_inicio", "fecha_fin"}  # Añade otros campos aquí

    if name in force_as_string or any(isinstance(v, str) for v in values):
        # Trata todo como string
        encoded_values = [str(v).strip().encode('utf-8') for v in values]
        max_len = max(len(v) for v in encoded_values)

        dim_str_name = f"{name}_str_len"
        if dim_str_name not in group.dimensions:
            group.createDimension(dim_str_name, max_len)

        var = group.createVariable(name, 'S1', (dim_name, dim_str_name))
        arr = np.zeros((len(values), max_len), dtype='S1')

        for i, v in enumerate(encoded_values):
            arr[i, :len(v)] = np.array(list(v), dtype='S1')  # ✅ Corrección aquí

        var[:, :] = arr
    else:
        # Guardar como float
        arr = np.array(values, dtype=np.float32)
        var = group.createVariable(name, np.float32, (dim_name,))
        var[:] = arr


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
