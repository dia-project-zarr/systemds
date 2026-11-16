package org.apache.sysds.runtime.io;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.store.*;
import dev.zarr.zarrjava.v3.Array;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.sysds.conf.ConfigurationManager;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.data.DenseBlock;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import ucar.ma2.DataType;

import java.io.IOException;
import java.io.InputStream;

public class ReaderZarr extends MatrixReader {

    protected final static String ARRAY_TYPE = "array";
    protected final static int ZARR_VERSION = 3;

    protected final FileFormatPropertiesZarr _props;

    public ReaderZarr(FileFormatPropertiesZarr props) {
        _props = props;
    }

    @Override
    public MatrixBlock readMatrixFromHDFS(String fname, long rlen, long clen, int blen, long estnnz) throws IOException, DMLRuntimeException {

        //prepare file access
        JobConf job = new JobConf(ConfigurationManager.getCachedJobConf());
        Path path = new Path(fname);
        FileSystem fs = IOUtilFunctions.getFileSystem(path, job);

        //check existence and non-empty file
        checkValidInputFile(fs, path);

        //create custom store that wraps hadoop
        Store store = new HadoopZarrStore(fs, path);

        StoreHandle storeHandle = new StoreHandle(
                store,
                _props.getDatasetName()
        );

        Array arr;
        try {
            arr = Array.open(storeHandle);
            checkArrayValidity(arr);
        } catch (ZarrException e) {
            throw new DMLRuntimeException(e);
        }

        long[] shape = arr.metadata().shape;
        if (shape.length <= 0 || shape.length > 2) {
            //TODO add support for more dimensions by linearizing
            throw new DMLRuntimeException("Arrays must have 0, 1 or 2 dimensions");
        }

        MatrixBlock ret = assertDimensionality(arr, blen, estnnz);

        DenseBlock db = ret.getDenseBlock();
        int numRowsInChunk = arr.metadata().chunkShape()[0];

        try {
            readZarrMatrixFromHDFS(ret.getNumRows(), ret.getNumColumns(), numRowsInChunk, shape, arr, db);
        } catch (ZarrException e) {
            throw new DMLRuntimeException(e);
        }

        ret.recomputeNonZeros();
        ret.examSparsity();
        return ret;
    }

    private MatrixBlock assertDimensionality(Array arr, int blockLength, long estnnz) throws IOException {

        long[] shape = arr.metadata().shape;

        long numRows = -1;
        long numCols = -1;

        //TODO add support for more dimensions by linearizing
        if (shape.length == 0) {
            return null; //or return an empty block?
        } else if (shape.length == 1) {
            numRows = shape[0];
        } else if (shape.length == 2) {
            numRows = shape[0];
            numCols = shape[1];
        } else {
            throw new DMLRuntimeException("Arrays must have 0, 1 or 2 dimensions");
        }

        long estimateNNZ = estnnz >= 0 ? estnnz : numRows*numCols;

        MatrixBlock ret = createOutputMatrixBlock(
                numRows,
                numCols,
                blockLength,
                estimateNNZ,
                true,
                false
        );

        return ret;
    }

    private static void readZarrMatrixFromHDFS(long numRows, long numCols, int numRowsInChunk, long[] shape, Array zarr, DenseBlock db) throws ZarrException {

        for(long r = 0; r < numRows; r += numRowsInChunk) {
            long[] offsets = new long[shape.length];
            offsets[0] = r;

            long[] shapeCopy = shape.clone();
            shapeCopy[0] = Math.min(numRowsInChunk, numRows - r);

            //we only consider dim 0 (rows) for offsets and read all (linearized) columns for it
            //this should work for any number of dimensions
            double[] vals = (double[]) zarr
                    .read(offsets, shapeCopy, true)
                    .get1DJavaArray(DataType.DOUBLE);

            for(int i = 0; i < shapeCopy[0]; i++)
                System.arraycopy(
                        vals,
                        (int) (i * numCols),
                        db.values((int) r + i),
                        db.pos((int) r + i),
                        (int) numCols
                );
        }
    }

    @Override
    public MatrixBlock readMatrixFromInputStream(InputStream is, long rlen, long clen, int blen, long estnnz)
            throws IOException, DMLRuntimeException {

        throw new UnsupportedOperationException(
                "Reading Zarr from an input stream is not supported. Zarr is a directory-based format.");
    }

    private String[] splitKeys(Path path){
        //TODO ??
        return path.toString().split("/");
    }

    private static void checkArrayValidity(Array array) {
        if (!array.metadata().nodeType.equals(ARRAY_TYPE))
            throw new DMLRuntimeException(String.format("the provided path is not of type %s but %s", ARRAY_TYPE, array.metadata().nodeType));
        if (array.metadata().zarrFormat != ZARR_VERSION) {
            throw new DMLRuntimeException("the zarr file is not version v3");
        }

        if (dataIsNumeric(array)) {
            throw new DMLRuntimeException("specified dataset is not numeric");
        }
    }

    private static boolean dataIsNumeric(Array array) {
        return !array.metadata().dataType.getMA2DataType().isNumeric();
    }
}
