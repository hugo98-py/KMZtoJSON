# -*- coding: utf-8 -*-
"""
FastAPI service: recibe un KMZ vía POST y devuelve JSON con:
    - lon / lat
    - UTM (x, y, zona)
    - Región, Provincia, Comuna y Localidad
"""
import os, zipfile, tempfile, unicodedata
from typing import List
from pathlib import Path
import numpy as np 
import geopandas as gpd
import pandas as pd
from pyproj import Transformer
from shapely.geometry import Point
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# ─── Configuración FastAPI ──────────────────────────────────────────────
app = FastAPI(title="KMZ → UTM + DPA + Localidad API")

whitelist = ["https://preview.flutterflow.io", "http://localhost"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=whitelist,
    allow_origin_regex=r"https://.*\.flutterflow\.app",
    allow_methods=["POST"],
    allow_headers=["*"],
    max_age=3600,
)

# ─── 0. Rutas de shapefiles ─────────────────────────────────────────────
# Carpeta del archivo actual
ROOT_DIR = Path(__file__).resolve().parent

# Carpeta con los shapefiles (hermana del .py)
BASE = ROOT_DIR / "DPA_2023"
REG_SH  = BASE / "REGIONES"        / "REGIONES_v1.shp"
PROV_SH = BASE / "PROVINCIAS"      / "PROVINCIAS_v1.shp"
COM_SH  = BASE / "COMUNAS"         / "COMUNAS_v1.shp"
LOC_SH  = BASE / "Areas_Pobladas"  / "Areas_Pobladas.shp"   # ← nuevo

def quitar_tildes(txt: str) -> str:
    return (unicodedata.normalize("NFKD", txt)
            .encode("ASCII", "ignore")
            .decode("utf‑8")) if isinstance(txt, str) else txt

# Carga las capas (una sola vez al iniciar) ------------------------------
gdf_region    = gpd.read_file(REG_SH ).to_crs(4326)[["REGION",    "geometry"]]
gdf_provincia = gpd.read_file(PROV_SH).to_crs(4326)[["PROVINCIA", "geometry"]]
gdf_comuna    = gpd.read_file(COM_SH ).to_crs(4326)[["COMUNA",    "geometry"]]
gdf_localidad = gpd.read_file(LOC_SH ).to_crs(4326)[["Localidad", "geometry"]]  # usa el campo real

for col_df, col_name in [
        (gdf_region,    "REGION"),
        (gdf_provincia, "PROVINCIA"),
        (gdf_comuna,    "COMUNA"),
        (gdf_localidad, "Localidad"),
]:
    col_df[col_name] = col_df[col_name].apply(quitar_tildes)

# ─── 1. Utilidades ──────────────────────────────────────────────────────
def lonlat_to_utm(lon: float, lat: float) -> tuple[float, float, str]:
    zone  = int((lon + 180) // 6) + 1
    south = lat < 0
    epsg  = 32700 + zone if south else 32600 + zone
    e, n = Transformer.from_crs(4326, epsg, always_xy=True).transform(lon, lat)
    return e, n, f"{zone}{'S' if south else 'N'}"

def add_admin_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega región, provincia, comuna y localidad mediante spatial join."""
    gdf_pts = gpd.GeoDataFrame(
        df,
        geometry=[Point(xy) for xy in zip(df.lon, df.lat)],
        crs=4326
    )

    gdf_pts = gpd.sjoin(gdf_pts,
                        gdf_region.rename(columns={"REGION": "region"}),
                        predicate="within", how="left").drop(columns="index_right")

    gdf_pts = gpd.sjoin(gdf_pts,
                        gdf_provincia.rename(columns={"PROVINCIA": "provincia"}),
                        predicate="within", how="left").drop(columns="index_right")

    gdf_pts = gpd.sjoin(gdf_pts,
                        gdf_comuna.rename(columns={"COMUNA": "comuna"}),
                        predicate="within", how="left").drop(columns="index_right")

    gdf_pts = gpd.sjoin(gdf_pts,
                        gdf_localidad.rename(columns={"Localidad": "localidad"}),
                        predicate="within", how="left").drop(columns="index_right")

    return gdf_pts.drop(columns="geometry")

# ─── 2. Procesamiento del KMZ ───────────────────────────────────────────
def process_kmz_bytes(kmz_bytes: bytes) -> List[dict]:
    with tempfile.TemporaryDirectory() as tmp:
        # 1) guarda el .kmz
        path = Path(tmp, "upload.kmz")
        path.write_bytes(kmz_bytes)

        # 2) descomprime
        zipfile.ZipFile(path).extractall(tmp)

        # 3) localiza todos los .kml
        kml_files = [Path(r, f)
                     for r, _, files in os.walk(tmp)
                     for f in files if f.lower().endswith(".kml")]
        if not kml_files:
            raise ValueError("KMZ sin KML interno.")

        recs = []
        for kml in kml_files:
            gdf = gpd.read_file(kml, driver="KML")

            base = pd.DataFrame({
                "Name": gdf["Name"],
                "lon":  gdf.geometry.x,
                "lat":  gdf.geometry.y,
                "responsable": ""
            })

            # UTM
            utm = base.apply(
                lambda r: pd.Series(lonlat_to_utm(r.lon, r.lat),
                                    index=["xx", "yy", "UTM_zone"]),
                axis=1
            )
            base = pd.concat([base, utm], axis=1)

            # Región / Provincia / Comuna / Localidad
            base = add_admin_columns(base)
            base["localidad"] = base["localidad"].fillna("Sin localidad")
            
            recs.extend(
                base[["Name", "lon", "lat", "xx", "yy", "UTM_zone",
                      "region", "provincia", "comuna", "localidad",
                      "responsable"]]
                .to_dict(orient="records")
            )
        return recs

# ─── 3. Endpoint ────────────────────────────────────────────────────────
@app.post("/upload-kmz")
async def upload_kmz(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".kmz"):
        raise HTTPException(400, "Solo se aceptan archivos KMZ.")
    try:
        data = process_kmz_bytes(await file.read())
    except Exception as exc:
        raise HTTPException(500, f"Error procesando KMZ: {exc}") from exc
    return data

