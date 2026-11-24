package org.apache.sysds.runtime.io;

import dev.zarr.zarrjava.core.codec.core.BytesCodec;
import dev.zarr.zarrjava.v3.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.sysds.conf.ConfigurationManager;
import org.apache.sysds.runtime.compress.CompressedMatrixBlock;
import org.apache.sysds.runtime.data.DenseBlock;
import org.apache.sysds.runtime.instructions.ooc.OOCStream;
import org.apache.sysds.runtime.instructions.spark.data.IndexedMatrixValue;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.util.HDFSTool;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.store.*;
import dev.zarr.zarrjava.store.StoreHandle;

import java.io.IOException;

public class WriterZarr extends MatrixWriter{

  // chunk size maps cleanly on default matrix-block size
  public static final int DEFAULT_CHUNK_SIZE = 1000;

  protected final FileFormatPropertiesZarr _props;
  private final int _chunkSize; // rows = cols per chunk (clamped to the matrix)
  private final boolean _gzip; // Maybe Zarr-Datasets are Gzip/Blosc compressed, supporting it is required
  private final boolean _parallel; // defines how zarr-java encodes/writes chunks in parallel

  public WriterZarr(FileFormatPropertiesZarr props) {
    this(props, DEFAULT_CHUNK_SIZE, false, false);
  }

  public WriterZarr(FileFormatPropertiesZarr props, int chunkSize, boolean gzip, boolean parallel) {
    if (chunkSize <= 0) {
      throw new IllegalArgumentException("Invalid zarr chunk size: " + chunkSize);
    }
    _props = props;
    _chunkSize = chunkSize;
    _gzip = gzip;
    _parallel = parallel;
  }

  @Override
  public void writeMatrixToHDFS(MatrixBlock src, String fname, long rlen, long clen, int blen, long nnz, boolean diag) throws IOException {
    if (src.getNumRows() != rlen || src.getNumColumns() != clen) {
      throw new IOException("Matrix dimensions mismatch with metadata: " + src.getNumRows() + "x"
        + src.getNumColumns() + " vs " + rlen + "x" + clen + ".");
    }
    if (diag) {
     throw new IOException("Diagonal write for zarr format not supported.");
    }
    if(src instanceof CompressedMatrixBlock)
      src = CompressedMatrixBlock.getUncompressed(src, "zarr write");

    // Create Zarr Array
    int cr = chunkDim(rlen), cc = chunkDim(clen);
    Array zarr = createZarrArray(fname, rlen, clen, cr, cc);

    if (src.isEmptyBlock(false)) {
      return; // Zarr-File initialized with Metadata and every val = 0.0
    }

    try {
      if (src.isInSparseFormat() || src.getDenseBlock().isContiguous()) {
        // for sparse block or very large dense blocks - write one chunk-sized slice at a time
        for (int r = 0; r < rlen; r += cr) {
          for (int c = 0; c < clen; c += cc) {
            // calc upper row/col of the current chunk
            int rowUpper = (int) Math.min(r + cr, rlen) - 1; // [0;rlen-1]
            int colUpper = (int) Math.min(c + cc, clen) -1; // [0;clen-1]
            writeBlock(src.slice(r, rowUpper, c, colUpper), zarr, r, c);
          }
        }
      } else {
        zarr.write(new long[]{0, 0}, wrap(src.getDenseBlockValues(), (int) rlen, (int) clen), _parallel);
      }
    } catch (RuntimeException e) {
      throw new IOException("Failed to write zarr array " + fname, e);
    }

  }

  @Override
  public long writeMatrixFromStream(String fname, OOCStream<IndexedMatrixValue> stream, long rlen, long clen, int blen) throws IOException {
    return 0;
  }

  @Override
  public void writeEmptyMatrixToHDFS(String fname, long rlen, long clen, int blen) throws IOException {

  }

  private int chunkDim(long dim) {
    return (int) Math.max(1, Math.min(_chunkSize, dim));
  }

  private Array createZarrArray(String fname, long rlen, long clen, int cr, int cc) throws IOException {
    // prepare file access
    JobConf job = new JobConf(ConfigurationManager.getCachedJobConf());
    Path p = new Path(fname);
    FileSystem fs = IOUtilFunctions.getFileSystem(p, job);

    HDFSTool.deleteFileIfExistOnHDFS(p, job);
    fs.mkdirs(p);

    // Create Store (= Physical Storage of the File)
    Store store = new HadoopZarrStore(fs, p);
    StoreHandle handle;
    String dataset = getDatasetName();

    if (dataset != null) {
      Group.create(new StoreHandle(store));
      handle = new StoreHandle(store, dataset);
    } else {
      handle = new StoreHandle(store);
    }

    try {
      ArrayMetadata md = Array.metadataBuilder()
        .withShape(rlen, clen)
        .withDataType(DataType.FLOAT64)
        .withChunkShape(cr, cc)
        .withFillValue(0.0)
        // Gzip Level 1 = ...
        .withCodecs(c -> _gzip ? c.withBytes(BytesCodec.Endian.LITTLE).withGzip(1) : c.withBytes(BytesCodec.Endian.LITTLE))
        .build();
      return Array.create(handle, md);
    } catch (ZarrException e) {
      throw new IOException(e);
    }
  }

  private void writeBlock(MatrixBlock src, Array dst, long rowOffset, long colOffset) {
    int r = src.getNumRows();
    int c = src.getNumColumns();
    double[] vals;
    DenseBlock db = src.getDenseBlock();

    if (!src.isInSparseFormat() || db != null && db.isContiguous()) {
      // Very large dense blocks
      vals = src.getDenseBlockValues();
    } else {
      // Handle Sparse MatrixBlocks by densifying
      MatrixBlock d = new MatrixBlock(r, c, false);
      d.copy(src);
      if (d.isInSparseFormat()) {
        d.sparseToDense();
      }
      vals = d.getDenseBlockValues();
    }

    if (vals == null) {
      return;
    }

    dst.write(new long[]{rowOffset, colOffset}, wrap(vals, r, c), _parallel);
  }

  private String getDatasetName() {
    String name = (_props != null) ? _props.getDatasetName() : null;
    return (name == null || name.isEmpty()) ? null : name;
  }

  private static ucar.ma2.Array wrap(double[] vals, int rows, int cols) {
    return ucar.ma2.Array.factory(ucar.ma2.DataType.DOUBLE, new int[] {rows, cols}, vals);
  }
}
