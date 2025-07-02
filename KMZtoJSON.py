# -*- coding: utf-8 -*-
"""
Created on Wed Jul  2 01:04:53 2025

@author: Hugo
"""

"""
FastAPI service: recibe un KMZ vía POST y devuelve coordenadas UTM en JSON.
"""

import os, zipfile, tempfile
from typing import List

import pandas as pd
import geopandas as gpd
from pyproj import Transformer
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="KMZ → UTM API")

# Orígenes explícitos (exact match)
whitelist = [
    "https://preview.flutterflow.io",
    "http://localhost",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=whitelist,                         # matches exact hosts
    allow_origin_regex=r"https://.*\.flutterflow\.app",  # cualquier subdominio *.flutterflow.app
    allow_methods=["POST"],                         # limita a POST; agrega "OPTIONS" si quieres manejar pre-flight tú
    allow_headers=["*"],                            # deja pasar todos los headers
    max_age=3600,                                   # caché del pre-flight (1 h)
)

# ─── Utilidades ──────────────────────────────────────────────────────────
def lonlat_to_utm(lon: float, lat: float) -> tuple[float, float, str]:
    zone  = int((lon + 180) // 6) + 1
    south = lat < 0
    epsg  = 32700 + zone if south else 32600 + zone
    e, n = Transformer.from_crs(4326, epsg, always_xy=True).transform(lon, lat)
    return e, n, f"{zone}{'S' if south else 'N'}"

def process_kmz_bytes(kmz_bytes: bytes) -> List[dict]:
    with tempfile.TemporaryDirectory() as tmp:
        # 1. guardar kmz
        path = os.path.join(tmp, "upload.kmz")
        with open(path, "wb") as f:
            f.write(kmz_bytes)

        # 2. descomprimir
        zipfile.ZipFile(path).extractall(tmp)

        # 3. localizar KML
        kml_files = [
            os.path.join(r, f)
            for r, _, files in os.walk(tmp)
            for f in files if f.lower().endswith(".kml")
        ]
        if not kml_files:
            raise ValueError("KMZ sin KML interno.")

        recs = []
        for kml in kml_files:
            gdf = gpd.read_file(kml, driver="KML")

            # --- DataFrame base ---
            tmp_df = pd.DataFrame({
                "Name": gdf["Name"],
                "lon":  gdf.geometry.x,
                "lat":  gdf.geometry.y,
                "responsable": ""
            })

            # --- columnas UTM ---
            utm = tmp_df.apply(
                lambda r: pd.Series(lonlat_to_utm(r.lon, r.lat),
                                    index=["xx", "yy", "UTM_zone"]),
                axis=1
            )
            tmp_df = pd.concat([tmp_df, utm], axis=1)

            # --- añadir al resultado ---
            recs.extend(
                tmp_df[["Name", "lon", "lat", "xx", "yy",
                        "UTM_zone", "responsable"]]
                .to_dict(orient="records")
            )

        return recs

# ─── Endpoint ────────────────────────────────────────────────────────────
@app.post("/upload-kmz")
async def upload_kmz(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".kmz"):
        raise HTTPException(400, "Solo se aceptan archivos KMZ.")
    try:
        data = process_kmz_bytes(await file.read())
    except Exception as exc:
        raise HTTPException(500, f"Error procesando KMZ: {exc}") from exc
    return data

