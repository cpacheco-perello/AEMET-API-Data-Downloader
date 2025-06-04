from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
import json
from netCDF4 import Dataset
import numpy as np
import os
import uuid

app = FastAPI()

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

        return FileResponse(tmp_filename, filename="datos.nc", media_type="application/netcdf")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creando NetCDF: {e}")
    finally:
        if os.path.exists(tmp_filename):
            os.remove(tmp_filename)
