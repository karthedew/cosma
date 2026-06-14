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

Most array fixtures are written UNCOMPRESSED (v2 compressor=None / v3 no
compression codec) so that chunk bytes are trivially computable golden values
for the Go ReadChunk tests (decode itself is Z5, out of this driver's scope).

The COMPRESSED and F-order fixtures (v2_codecs, v3_codecs, v2_order) were added
for the Z5 chunk-decode pipeline tests (internal/ndingest). They hold the same
small int64/float64 arrays compressed with gzip/zstd/lz4 (v2: numcodecs
GZip/Zstd/LZ4; v3: GzipCodec/ZstdCodec chains) and an F-order array with a
byte-identical C-order twin. The Go decode tests check that these round-trip
back to the known generator values. They do not change any pre-existing fixture
store, so `git status` over the previously committed testdata stays clean.

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


# The known golden values shared by every Z5 codec fixture. Kept tiny so the Go
# tests can embed them literally.
CODEC_I8 = np.array([-3, -1, 0, 1, 2, 7, 100, 200], dtype="<i8")
CODEC_F8 = np.array([1.5, -2.5, 3.25, 0.0, 4.5, -6.75, 7.0, 8.125], dtype="<f8")

# A larger, deterministic, compressible int64 array for the Z8 blosc fixtures.
# The Go test reconstructs the same pattern: value i is (i % 17) - 4, which has
# only 17 distinct values so it compresses well (non-memcpy frames) yet is not a
# constant run, so the inner codec and shuffle both do real work.
BLOSC_BIG_I8 = np.array([(i % 17) - 4 for i in range(1024)], dtype="<i8")


def v2_codecs():
    """v2 store: the same small int64/float64 arrays compressed with gzip, zstd,
    and lz4 (numcodecs GZip/Zstd/LZ4). Two chunks each so the codec runs on more
    than a single chunk. Z5 decode tests round-trip these to CODEC_I8/CODEC_F8."""
    import numcodecs

    root = fresh("v2_codecs")
    g = zarr.open_group(root, mode="w", zarr_format=2)
    g.attrs["title"] = "v2 codec fixtures"
    codecs = {
        "gzip": numcodecs.GZip(level=5),
        "zstd": numcodecs.Zstd(level=3),
        "lz4": numcodecs.LZ4(acceleration=1),
    }
    for cname, comp in codecs.items():
        ai = g.create_array(
            f"{cname}_i8", shape=(8,), chunks=(4,), dtype="<i8", compressors=comp,
        )
        ai[:] = CODEC_I8
        ai.attrs["_ARRAY_DIMENSIONS"] = ["i"]
        af = g.create_array(
            f"{cname}_f8", shape=(8,), chunks=(4,), dtype="<f8", compressors=comp,
        )
        af[:] = CODEC_F8
        af.attrs["_ARRAY_DIMENSIONS"] = ["i"]
    return root


def v3_codecs():
    """v3 store: int64/float64 arrays with gzip and zstd codec chains
    (bytes -> {gzip|zstd}). Z5 decode tests round-trip these."""
    from zarr.codecs import GzipCodec, ZstdCodec

    root = fresh("v3_codecs")
    g = zarr.open_group(root, mode="w", zarr_format=3)
    g.attrs["title"] = "v3 codec fixtures"
    codecs = {
        "gzip": [GzipCodec(level=5)],
        "zstd": [ZstdCodec(level=3)],
    }
    for cname, comp in codecs.items():
        ai = g.create_array(
            f"{cname}_i8", shape=(8,), chunks=(4,), dtype="int64",
            compressors=comp, dimension_names=["i"],
        )
        ai[:] = CODEC_I8
        af = g.create_array(
            f"{cname}_f8", shape=(8,), chunks=(4,), dtype="float64",
            compressors=comp, dimension_names=["i"],
        )
        af[:] = CODEC_F8
    return root


def v2_order():
    """v2 store: an F-order (column-major) array and its C-order (row-major) twin
    holding identical values, each in a single chunk. Z5 transposes the F-order
    chunk to C-order at decode time and the test asserts the two match
    cell-for-cell. Uncompressed so the transpose is the only variable."""
    root = fresh("v2_order")
    g = zarr.open_group(root, mode="w", zarr_format=2)
    g.attrs["title"] = "v2 order fixtures"
    vals = np.arange(24, dtype="<i8").reshape(4, 6)

    f = g.create_array(
        "f_order", shape=(4, 6), chunks=(4, 6), dtype="<i8",
        compressors=None, order="F",
    )
    f[:] = vals
    f.attrs["_ARRAY_DIMENSIONS"] = ["y", "x"]

    c = g.create_array(
        "c_order", shape=(4, 6), chunks=(4, 6), dtype="<i8",
        compressors=None, order="C",
    )
    c[:] = vals
    c.attrs["_ARRAY_DIMENSIONS"] = ["y", "x"]
    return root


def v2_blosc():
    """v2 store: the same small int64/float64 arrays compressed with numcodecs
    Blosc across the (cname, shuffle) combinations a real store emits. Two chunks
    each so the block loop runs on more than one block per chunk path.

    Z8 decode tests round-trip every array back to CODEC_I8/CODEC_F8. The most
    important combo is blosclz + byte-shuffle: that is the Zarr v2 default that
    real-world stores use. We also cover lz4 + shuffle, zstd + shuffle, lz4
    without shuffle, and one bitshuffle case. Array names encode the combo as
    "{cname}_{shuffle}_{dtype}", e.g. blosclz_shuffle_i8.

    Provenance: zarr==3.0.8, numcodecs==0.16.5. Frames are blosc1 (c-blosc
    version 1 container) — the format this decoder targets.
    """
    import numcodecs
    from numcodecs import Blosc

    root = fresh("v2_blosc")
    g = zarr.open_group(root, mode="w", zarr_format=2)
    g.attrs["title"] = "v2 blosc fixtures"

    # (label, cname, shuffle-flag)
    combos = [
        ("blosclz_shuffle", "blosclz", Blosc.SHUFFLE),     # zarr v2 default
        ("lz4_shuffle", "lz4", Blosc.SHUFFLE),
        ("zstd_shuffle", "zstd", Blosc.SHUFFLE),
        ("lz4_noshuffle", "lz4", Blosc.NOSHUFFLE),
        ("blosclz_bitshuffle", "blosclz", Blosc.BITSHUFFLE),
        ("zlib_shuffle", "zlib", Blosc.SHUFFLE),
    ]
    for label, cname, shuffle in combos:
        comp = Blosc(cname=cname, clevel=5, shuffle=shuffle)
        ai = g.create_array(
            f"{label}_i8", shape=(8,), chunks=(4,), dtype="<i8", compressors=comp,
        )
        ai[:] = CODEC_I8
        ai.attrs["_ARRAY_DIMENSIONS"] = ["i"]
        af = g.create_array(
            f"{label}_f8", shape=(8,), chunks=(4,), dtype="<f8", compressors=comp,
        )
        af[:] = CODEC_F8
        af.attrs["_ARRAY_DIMENSIONS"] = ["i"]

        # A larger, compressible companion array per combo. The tiny arrays above
        # are stored MEMCPYED (incompressible), so they never exercise the
        # inner-codec block path or the shuffle inversion. BIG_I8 is highly
        # repetitive, forcing real compression (non-memcpy frames with byte/bit
        # shuffle applied) across multiple blocks/chunks.
        big = g.create_array(
            f"{label}_big_i8", shape=(1024,), chunks=(512,), dtype="<i8",
            compressors=comp,
        )
        big[:] = BLOSC_BIG_I8
        big.attrs["_ARRAY_DIMENSIONS"] = ["i"]

    # One multi-block-per-chunk fixture: a forced small blocksize (256 bytes)
    # splits each 512-element (4096-byte) chunk into many blosc blocks, exercising
    # the per-block offset table and block loop. Uses the v2 default codec.
    mb = Blosc(cname="blosclz", clevel=5, shuffle=Blosc.SHUFFLE, blocksize=256)
    mblock = g.create_array(
        "multiblock_i8", shape=(1024,), chunks=(512,), dtype="<i8", compressors=mb,
    )
    mblock[:] = BLOSC_BIG_I8
    mblock.attrs["_ARRAY_DIMENSIONS"] = ["i"]
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
    # Z5 chunk-decode fixtures (compressed + F-order).
    v2_codecs()
    v3_codecs()
    v2_order()
    # Z8 blosc fixtures.
    v2_blosc()
    print("fixtures written under", HERE)


if __name__ == "__main__":
    main()
