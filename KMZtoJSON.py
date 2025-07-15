# -*- coding: utf-8 -*-
"""
FastAPI | KMZ ➜ JSON  ·  lon/lat · UTM · Región · Provincia · Comuna · Localidad
Pensado para correr tal cual desde GitHub/Render:
    • Los shapefiles viven dentro del mismo repo (ruta relativa).
    • Opcional: DPA_DIR env‑var para apuntar a otra carpeta (p. ej. un volumen).
"""

import os, zipfile, tempfile, unicodedata
from pathlib import Path
from typing import List

import geopandas as gpd
import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pyproj import Transformer
from shapely.geometry import Point

# ─────────────────────── FastAPI & CORS ────────────────────────────────
app = FastAPI(title="KMZ → UTM + DPA + Localidad API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://preview.flutterflow.io",
        "http://localhost",
    ],
    allow_origin_regex=r"https://.*\.flutterflow\.app",
    allow_methods=["POST"],
    allow_headers=["*"],
    max_age=3600,
)

# ─────────────────────── Rutas a los shapefiles ────────────────────────
ROOT_DIR = Path(__file__).resolve().parent           # carpeta del .py
BASE     = Path(os.getenv("RECORTADAS_CENTRO_NORTE", ROOT_DIR))      # override opcional

COM_SH = BASE / "COMUNAS"      / "COMUNAS_CENTRO_NORTE.shp"
LOC_SH = BASE / "LOCALIDADES"  / "ZONAS_POBLADAS_CENTRO_NORTE.shp"

# ─────────────────────── Utilidades de texto ───────────────────────────
def quitar_tildes(txt: str) -> str:
    return (unicodedata.normalize("NFKD", txt).encode("ascii", "ignore").decode()
            if isinstance(txt, str) else txt)

# ─────────────────────── Carga capas (una vez) ─────────────────────────
try:
    gdf_comuna = gpd.read_file(COM_SH).to_crs(4326)[["REGION", "PROVINCIA", "COMUNA", "geometry"]]
except Exception as e:
    raise RuntimeError(f"No se pudo abrir {COM_SH}: {e}")

try:
    gdf_localidad = gpd.read_file(LOC_SH).to_crs(4326)[["Localidad", "geometry"]]
except Exception as e:
    raise RuntimeError(f"No se pudo abrir {LOC_SH}: {e}")

gdf_comuna[["REGION", "PROVINCIA", "COMUNA"]] = gdf_comuna[
    ["REGION", "PROVINCIA", "COMUNA"]].applymap(quitar_tildes)
gdf_localidad["Localidad"] = gdf_localidad["Localidad"].apply(quitar_tildes)

# ─────────────────────── Geom. y proyección ────────────────────────────
def lonlat_to_utm(lon: float, lat: float):
    zone  = int((lon + 180) // 6) + 1
    south = lat < 0
    epsg  = 32700 + zone if south else 32600 + zone
    e, n  = Transformer.from_crs(4326, epsg, always_xy=True).transform(lon, lat)
    return e, n, f"{zone}{'S' if south else 'N'}"

def add_admin_cols(df: pd.DataFrame) -> pd.DataFrame:
    pts = gpd.GeoDataFrame(df, geometry=[Point(xy) for xy in zip(df.lon, df.lat)], crs=4326)

    pts = gpd.sjoin(
        pts,
        gdf_comuna.rename(columns={
            "REGION":    "region",
            "PROVINCIA": "provincia",
            "COMUNA":    "comuna",
        }),
        how="left", predicate="within"
    ).drop(columns="index_right")

    pts = gpd.sjoin(
        pts,
        gdf_localidad.rename(columns={"Localidad": "localidad"}),
        how="left", predicate="within"
    ).drop(columns="index_right")

    return pts.drop(columns="geometry")

# ─────────────────────── Procesa el KMZ ────────────────────────────────
def process_kmz_bytes(kmz: bytes) -> List[dict]:
    with tempfile.TemporaryDirectory() as tmp:
        kmz_path = Path(tmp, "upload.kmz")
        kmz_path.write_bytes(kmz)
        zipfile.ZipFile(kmz_path).extractall(tmp)

        kmls = [Path(r, f) for r, _, files in os.walk(tmp)
                for f in files if f.lower().endswith(".kml")]
        if not kmls:
            raise ValueError("KMZ sin KML interno.")

        recs = []
        for kml in kmls:
            gdf = gpd.read_file(kml, driver="KML")

            base = pd.DataFrame({
                "Name": gdf["Name"],
                "lon":  gdf.geometry.x,
                "lat":  gdf.geometry.y,
                "responsable": "",
            })

            base = pd.concat(
                [
                    base,
                    base.apply(lambda r: pd.Series(
                        lonlat_to_utm(r.lon, r.lat),
                        index=["xx", "yy", "UTM_zone"]),
                        axis=1),
                ],
                axis=1,
            )

            base = add_admin_cols(base)
            base["localidad"] = base["localidad"].fillna("Sin localidad")

            recs.extend(
                base[[
                    "Name", "lon", "lat", "xx", "yy", "UTM_zone",
                    "region", "provincia", "comuna", "localidad",
                    "responsable",
                ]].to_dict(orient="records")
            )

        return recs

# ─────────────────────── Endpoint ───────────────────────────────────────
@app.post("/upload-kmz")
async def upload_kmz(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".kmz"):
        raise HTTPException(400, "Solo se aceptan archivos KMZ.")
    try:
        return process_kmz_bytes(await file.read())
    except Exception as exc:
        raise HTTPException(500, f"Error procesando KMZ: {exc}") from exc

