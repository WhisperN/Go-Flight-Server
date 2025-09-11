from __future__ import annotations

import io
import os
import tempfile
import pandas as pd
from pathlib import Path
from numpy.distutils.lib2def import output_def

try:
    # chardet ist nützlich, aber optional. Wenn nicht vorhanden, fahren wir mit Fallbacks fort.
    import chardet
except ImportError:
    chardet = None


def _remove_null_bytes(path: str) -> str:
    """Erstellt eine temporäre Kopie ohne NUL-Bytes. Gibt Pfad zurück."""
    with open(path, "rb") as src, tempfile.NamedTemporaryFile(delete=False) as tmp:
        for chunk in iter(lambda: src.read(1 << 20), b""):
            tmp.write(chunk.replace(b"\x00", b""))
        return tmp.name


def _maybe_is_binary_or_compressed(path: str) -> bool:
    """Grober Check, ob Datei binär/komprimiert wirkt (kein verlässlicher Test)."""
    with open(path, "rb") as f:
        head = f.read(8)
    # ZIP, GZIP, ZSTD, PARQUET/ARROW (nur grob)
    return head.startswith(b"PK\x03\x04") or head.startswith(b"\x1f\x8b") or \
        head.startswith(b"\x28\xb5\x2f\xfd") or head.startswith(b"PAR1")


def _detect_encoding(path: str) -> str | None:
    if chardet is None:
        return None
    with open(path, "rb") as f:
        raw = f.read(200_000)  # sample
    guess = chardet.detect(raw)
    enc = guess.get("encoding")
    # chardet kann manchmal Mist liefern; None zurückgeben, wenn unsicher
    return enc if enc else None


def _read_tsv_robust(path: str, usecols: list[str], na_values: list[str] | None = None) -> pd.DataFrame:
    """
    Robustes TSV-Lesen mit:
    - NUL-Filter
    - Encoding-Erkennung (chardet) + Fallbacks
    - engine='c'
    """
    if _maybe_is_binary_or_compressed(path):
        raise ValueError(
            f"Die Datei {path!r} wirkt komprimiert/binär. "
            "Bitte zuerst entpacken (z.B. .zip/.gz) oder das richtige Format übergeben."
        )

    # 1) NUL-Bytes entfernen (temporäre Datei)
    clean_path = _remove_null_bytes(path)

    # 2) Encoding-Strategie
    encodings_to_try = []
    detected = _detect_encoding(clean_path)
    if detected:
        encodings_to_try.append(detected)
    # sinnvolle Fallbacks
    encodings_to_try += ["utf-8", "utf-8-sig", "latin1", "cp1252"]

    last_err = None
    for enc in encodings_to_try:
        try:
            df = pd.read_csv(
                clean_path,
                sep="\t",
                usecols=usecols,
                na_values=na_values or ["NA", "NaN", "", "null", "NULL"],
                engine="c",
                encoding=enc,
                dtype_backend="numpy_nullable",  # moderne pandas: Null-fähig
                low_memory=False,
                on_bad_lines="skip",  # falls ganz wilde Zeilen existieren
            )
            return df
        except Exception as e:  # noqa: BLE001
            last_err = e
    # Wenn alle Encodings scheitern, Fehler werfen
    raise last_err if last_err else RuntimeError(f"Unbekannter Fehler beim Lesen von {path!r}")


def join_splot_txt_to_parquet(
        header_path: str,
        dt_path: str,
        out_path: str = "joined_splot.parquet"
) -> None:
    """
    Liest zwei TAB-getrennte .txt (header + DT), joint über PlotObservationID
    und schreibt Parquet mit den Spalten:
    PlotObservationID, Biome, Longitude, Latitude, Species, Relative_cover.
    """
    header_cols = ["PlotObservationID", "Biome", "Longitude", "Latitude"]
    dt_cols     = ["PlotObservationID", "Species", "Relative_cover"]

    header = _read_tsv_robust(header_path, usecols=header_cols)
    dt     = _read_tsv_robust(dt_path,     usecols=dt_cols)

    # Dtypes (sanft) korrigieren
    # PlotObservationID als integer (nullable), numerische Spalten casten
    if "PlotObservationID" in header.columns:
        header["PlotObservationID"] = pd.to_numeric(header["PlotObservationID"], errors="coerce").astype("Int64")
    if "PlotObservationID" in dt.columns:
        dt["PlotObservationID"] = pd.to_numeric(dt["PlotObservationID"], errors="coerce").astype("Int64")

    df = header.merge(dt, on="PlotObservationID", how="inner")

    # Existenzcheck der Zielspalten
    need = ["PlotObservationID", "Biome", "Longitude", "Latitude", "Species", "Relative_cover"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise KeyError(f"Fehlende Spalten nach Join: {missing}. Spalten vorhanden: {list(df.columns)}")

    # Numerische Spalten sauber konvertieren (float32 spart Platz)
    for col in ("Longitude", "Latitude", "Relative_cover"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")

    # (Optional) einfache Range-Checks
    bad_lon = (~df["Longitude"].between(-180, 180)) & df["Longitude"].notna()
    bad_lat = (~df["Latitude"].between(-90, 90)) & df["Latitude"].notna()
    if bad_lon.any() or bad_lat.any():
        print(f"⚠️  Warnung: {bad_lon.sum()} Longitude- und {bad_lat.sum()} Latitude-Werte außerhalb der erwarteten Range.")

    # Parquet schreiben
    df[need].to_parquet(out_path, index=False)
    print(f"✅ Join abgeschlossen: {len(df):,} Zeilen → {out_path}")


if __name__ == "__main__":
    header_file = Path("/Users/nils/Documents/UZH/FS25/bsc/Go-Flight-Server/internal/data/3474_76_sPlotOpen_header(3).txt")
    dt_file     = Path("/Users/nils/Documents/UZH/FS25/bsc/Go-Flight-Server/internal/data/3474_76_sPlotOpen_DT(2).txt")
    output      = Path("/Users/nils/Documents/UZH/FS25/bsc/Go-Flight-Server/third_party/dataset/sPlotPositions.parquet")
    join_splot_txt_to_parquet(
        header_path=str(header_file),
        dt_path=str(dt_file),
        out_path=str(output)
    )

    print("Fertig!")