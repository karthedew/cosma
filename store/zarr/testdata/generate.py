#!/usr/bin/env python3
"""Regenerate the committed Zarr fixtures for the store/zarr driver tests.

Canonical generator. The output under store/zarr/testdata/ is committed so the
Go tests need no Python; run this only to regenerate after a deliberate change.

Pinned toolchain (matches the committed output):
    zarr==3.0.8
    numpy>=2

    python3 -m venv /tmp/zarrvenv
    /tmp/zarrvenv/bin/pip install "zarr==3.0.8" "numpy>=2"
    /tmp/zarrvenv/bin/python store/zarr/testdata/generate.py

All array fixtures are written UNCOMPRESSED (v2 compressor=None / v3 no
compression codec) so that chunk bytes are trivially computable golden values
for the Go ReadChunk tests (decode itself is Z5, out of this driver's scope).

zarr-python 3.x writes both Zarr v2 (zarr_format=2) and v3 (zarr_format=3)
on-disk layouts. For v3 it defaults to chunk_key_encoding "default" with a "/"
separator; we additionally emit the "default"+"." and "v2" variants explicitly.
"""

import os
import shutil

import numpy as np
import zarr
from zarr.core.chunk_key_encodings import DefaultChunkKeyEncoding, V2ChunkKeyEncoding

HERE = os.path.dirname(os.path.abspath(__file__))


def fresh(name):
    path = os.path.join(HERE, name)
    shutil.rmtree(path, ignore_errors=True)
    return path


def v2_nested(sep):
    """v2 store: nested groups, attrs, a chosen dimension_separator, dtype
    spread, a NaN-fill array, a missing-chunk array, a 0-d scalar, and a
    big-endian array. `sep` is "." or "/"."""
    suffix = "dot" if sep == "." else "slash"
    cke = V2ChunkKeyEncoding(separator=sep)
    root = fresh(f"v2_nested_{suffix}")
    g = zarr.open_group(root, mode="w", zarr_format=2)
    g.attrs["title"] = f"v2 nested ({sep})"
    g.attrs["level"] = 0

    # /temp: 2-D float64 little-endian, 2x2 chunks over a 4x4 grid, named dims.
    temp = g.create_array(
        "temp", shape=(4, 4), chunks=(2, 2), dtype="<f8",
        compressors=None, chunk_key_encoding=cke,
    )
    temp[:] = np.arange(16, dtype="<f8").reshape(4, 4)
    temp.attrs["_ARRAY_DIMENSIONS"] = ["y", "x"]
    temp.attrs["units"] = "kelvin"

    # nested subgroup with its own array
    sub = g.create_group("sub")
    sub.attrs["note"] = "nested group"
    counts = sub.create_array(
        "counts", shape=(3,), chunks=(2,), dtype="<i8",
        compressors=None, chunk_key_encoding=cke,
    )
    counts[:] = [100, 200, 300]
    counts.attrs["_ARRAY_DIMENSIONS"] = ["t"]

    return root


def v2_consolidated_pair():
    """Two byte-identical v2 stores differing only in the presence of
    .zmetadata, to prove consolidated == unconsolidated Trees."""
    def build(root):
        g = zarr.open_group(root, mode="w", zarr_format=2)
        g.attrs["title"] = "consolidated pair"
        a = g.create_array("a", shape=(4,), chunks=(2,), dtype="<i4", compressors=None)
        a[:] = [1, 2, 3, 4]
        a.attrs["_ARRAY_DIMENSIONS"] = ["n"]
        sub = g.create_group("grp")
        b = sub.create_array("b", shape=(2, 2), chunks=(1, 2), dtype="<f4", compressors=None)
        b[:] = [[1.5, 2.5], [3.5, 4.5]]
        return g

    plain = fresh("v2_unconsolidated")
    build(plain)

    cons = fresh("v2_consolidated")
    g = build(cons)
    zarr.consolidate_metadata(g.store, zarr_format=2)
    return plain, cons


def v2_dtypes():
    """Every supported v2 dtype as a tiny 1-D array, plus a big-endian f8."""
    root = fresh("v2_dtypes")
    g = zarr.open_group(root, mode="w", zarr_format=2)
    specs = {
        "i4": ("<i4", np.array([-1, 0, 1, 2], dtype="<i4")),
        "i8": ("<i8", np.array([-1, 0, 1, 2], dtype="<i8")),
        "f4": ("<f4", np.array([1.5, 2.5, 3.5, 4.5], dtype="<f4")),
        "f8": ("<f8", np.array([1.5, 2.5, 3.5, 4.5], dtype="<f8")),
        "bool": ("|b1", np.array([True, False, True, False], dtype="|b1")),
        "u1": ("|u1", np.array([0, 1, 200, 255], dtype="|u1")),
        "be_f8": (">f8", np.array([1.5, 2.5, 3.5, 4.5], dtype=">f8")),
    }
    for name, (dt, data) in specs.items():
        a = g.create_array(name, shape=(4,), chunks=(2,), dtype=dt, compressors=None)
        a[:] = data
        a.attrs["_ARRAY_DIMENSIONS"] = ["i"]
    return root


def v2_fillvalue_nan():
    """v2 array whose fill_value is NaN (serialized as the JSON string "NaN")."""
    root = fresh("v2_fill_nan")
    g = zarr.open_group(root, mode="w", zarr_format=2)
    a = g.create_array("f", shape=(4,), chunks=(2,), dtype="<f8",
                       compressors=None, fill_value=np.nan)
    # write only the first chunk so the second is a genuine missing (fill) chunk
    a[0:2] = [1.0, 2.0]
    a.attrs["_ARRAY_DIMENSIONS"] = ["i"]
    return root


def v2_missing_chunks():
    """v2 array that is partially written: some chunks present, some missing."""
    root = fresh("v2_missing")
    g = zarr.open_group(root, mode="w", zarr_format=2)
    a = g.create_array("sparse", shape=(6,), chunks=(2,), dtype="<i4", compressors=None)
    # write only chunk 0 and chunk 2; chunk 1 stays missing
    a[0:2] = [1, 2]
    a[4:6] = [5, 6]
    a.attrs["_ARRAY_DIMENSIONS"] = ["i"]
    return root


def v2_scalar():
    """v2 0-d scalar array (chunk key "0")."""
    root = fresh("v2_scalar")
    g = zarr.open_group(root, mode="w", zarr_format=2)
    a = g.create_array("sc", shape=(), chunks=(), dtype="<i4", compressors=None)
    a[...] = 42
    return root


def v2_xarray():
    """xarray-style group: 1-D coordinate arrays named after their own dim via
    _ARRAY_DIMENSIONS, plus a 2-D data variable. Reused by issue #9."""
    root = fresh("v2_xarray")
    g = zarr.open_group(root, mode="w", zarr_format=2)
    g.attrs["title"] = "xarray-style dataset"

    x = g.create_array("x", shape=(3,), chunks=(3,), dtype="<i8", compressors=None)
    x[:] = [0, 1, 2]
    x.attrs["_ARRAY_DIMENSIONS"] = ["x"]

    y = g.create_array("y", shape=(2,), chunks=(2,), dtype="<i8", compressors=None)
    y[:] = [10, 20]
    y.attrs["_ARRAY_DIMENSIONS"] = ["y"]

    data = g.create_array("data", shape=(2, 3), chunks=(2, 3), dtype="<f8", compressors=None)
    data[:] = np.arange(6, dtype="<f8").reshape(2, 3)
    data.attrs["_ARRAY_DIMENSIONS"] = ["y", "x"]
    return root


def v3_nested():
    """v3 store: nested groups, dim names, dtype spread, big-endian, scalar,
    missing chunks, NaN fill. Default chunk_key_encoding ("/" separator)."""
    root = fresh("v3_nested")
    g = zarr.open_group(root, mode="w", zarr_format=3)
    g.attrs["title"] = "v3 nested"

    temp = g.create_array(
        "temp", shape=(4, 4), chunks=(2, 2), dtype="float64",
        compressors=None, dimension_names=["y", "x"],
    )
    temp[:] = np.arange(16, dtype="<f8").reshape(4, 4)
    temp.attrs["units"] = "kelvin"

    sub = g.create_group("sub")
    sub.attrs["note"] = "nested v3 group"
    counts = sub.create_array(
        "counts", shape=(3,), chunks=(2,), dtype="int64",
        compressors=None, dimension_names=["t"],
    )
    counts[:] = [100, 200, 300]
    return root


def v3_dtypes():
    """v3 dtype spread plus a big-endian float64 (endian in the bytes codec)."""
    root = fresh("v3_dtypes")
    g = zarr.open_group(root, mode="w", zarr_format=3)
    for name, dt in [
        ("i4", "int32"), ("i8", "int64"), ("f4", "float32"),
        ("f8", "float64"), ("bool", "bool"), ("u1", "uint8"),
    ]:
        a = g.create_array(name, shape=(4,), chunks=(2,), dtype=dt,
                           compressors=None, dimension_names=["i"])
        if dt == "bool":
            a[:] = [True, False, True, False]
        elif dt == "float32" or dt == "float64":
            a[:] = [1.5, 2.5, 3.5, 4.5]
        else:
            a[:] = [0, 1, 2, 3]

    # big-endian float64: force the bytes codec endian to big.
    from zarr.codecs import BytesCodec
    be = g.create_array(
        "be_f8", shape=(4,), chunks=(2,), dtype="float64",
        compressors=None, serializer=BytesCodec(endian="big"),
        dimension_names=["i"],
    )
    be[:] = [1.5, 2.5, 3.5, 4.5]
    return root


def v3_key_encodings():
    """v3 array variants exercising every chunk_key_encoding the driver supports:
    default "/", default ".", and v2 "."."""
    root = fresh("v3_key_encodings")
    g = zarr.open_group(root, mode="w", zarr_format=3)

    d_slash = g.create_array(
        "default_slash", shape=(4,), chunks=(2,), dtype="int32", compressors=None,
        chunk_key_encoding=DefaultChunkKeyEncoding(separator="/"),
    )
    d_slash[:] = [1, 2, 3, 4]

    d_dot = g.create_array(
        "default_dot", shape=(4,), chunks=(2,), dtype="int32", compressors=None,
        chunk_key_encoding=DefaultChunkKeyEncoding(separator="."),
    )
    d_dot[:] = [1, 2, 3, 4]

    v2enc = g.create_array(
        "v2_enc", shape=(4,), chunks=(2,), dtype="int32", compressors=None,
        chunk_key_encoding=V2ChunkKeyEncoding(separator="."),
    )
    v2enc[:] = [1, 2, 3, 4]
    return root


def v3_scalar():
    """v3 0-d scalar array (default encoding chunk key "c")."""
    root = fresh("v3_scalar")
    g = zarr.open_group(root, mode="w", zarr_format=3)
    a = g.create_array("sc", shape=(), chunks=(), dtype="int32", compressors=None)
    a[...] = 42
    return root


def v3_missing_and_fill():
    """v3 array with a missing chunk and a NaN fill value."""
    root = fresh("v3_missing")
    g = zarr.open_group(root, mode="w", zarr_format=3)
    a = g.create_array("sparse", shape=(6,), chunks=(2,), dtype="float64",
                       compressors=None, fill_value=float("nan"),
                       dimension_names=["i"])
    a[0:2] = [1.0, 2.0]
    a[4:6] = [5.0, 6.0]
    return root


def main():
    v2_nested(".")
    v2_nested("/")
    v2_consolidated_pair()
    v2_dtypes()
    v2_fillvalue_nan()
    v2_missing_chunks()
    v2_scalar()
    v2_xarray()
    v3_nested()
    v3_dtypes()
    v3_key_encodings()
    v3_scalar()
    v3_missing_and_fill()
    print("fixtures written under", HERE)


if __name__ == "__main__":
    main()
