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

# ✅ CORS configurado justo después de crear la app
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Cambia esto si quieres restringir a tu dominio
    allow_methods=["*"],
    allow_headers=["*"],
)

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
        n = len(data.get("Datos", []))
        ds.createDimension("registro", n)

        valores = [float(d.get("valor", 0)) for d in data.get("Datos", [])]
        var = ds.createVariable("valor", np.float32, ("registro",))
        var[:] = valores

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
