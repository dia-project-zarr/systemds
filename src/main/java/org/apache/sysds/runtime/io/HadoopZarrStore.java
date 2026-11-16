package org.apache.sysds.runtime.io;

import dev.zarr.zarrjava.store.Store;
import dev.zarr.zarrjava.store.StoreException;
import dev.zarr.zarrjava.store.StoreHandle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * <code>HadoopZarrStore</code> abstracts the Hadoop file system that abstracts S3, HDFS, and Local File systems in SystemDS behind the Zarr <code>Store</code> abstraction.
 * It allows for reading (partial) files and writing files and is used in the background by the Zarr library to serialize and deserialize data.
 * It does not implement the methods of <code>ListableStore</code> as this class should only be used to represent Zarr arrays (i.e. "leaf nodes") and not Zarr groups.
 */
public class HadoopZarrStore implements Store {

    private final FileSystem fs;
    private final Path root;

    private static final Log LOG = LogFactory.getLog(HadoopZarrStore.class.getName());

    public HadoopZarrStore(FileSystem fs, Path root){
        this.fs = fs;
        this.root = root;
    }

    @Override
    public boolean exists(String[] keys) {
        return false;
    }

    @Override
    public @Nullable ByteBuffer get(String[] keys) {
        return get(keys, 0, -1);
    }

    @Override
    public @Nullable ByteBuffer get(String[] keys, long start) {
        return get(keys, start, -1);
    }

    @Override
    public @Nullable ByteBuffer get(String[] keys, long start, long end) {
        //TODO
        Path path = toPath(keys);
        try(FSDataInputStream in = fs.open(path)) {
            long len = fs.getFileStatus(path).getLen();
            // in zarr-java negative indices mean reading from the back
            long startPoint = start >= 0 ? start : len + start;
            long endPoint = end >= 0 ? end : len;
            //TODO this might be an issue
            byte[] buffer = new byte[(int) (endPoint-startPoint)];
            in.readFully(startPoint, buffer);
            return ByteBuffer.wrap(buffer);
        } catch(IOException e) {
            throw StoreException.readFailed(toString(), keys, e);
        }
    }

    @Override
    public void set(String[] keys, ByteBuffer bytes) {
        Path path = toPath(keys);
        try (FSDataOutputStream out = fs.create(path, true)) {
            out.write(bytes.array());
        }catch(IOException e) {
            throw StoreException.writeFailed(toString(), keys, e);
        }
    }

    @Override
    public void delete(String[] keys) {
        try {
            fs.delete(toPath(keys), true);
        } catch (IOException e) {
            throw StoreException.deleteFailed(toString(), keys, e);
        }

    }

    @Override
    public @NotNull StoreHandle resolve(String... keys) {
        return new StoreHandle(this, keys);
    }

    @Override
    public InputStream getInputStream(String[] keys, long start, long end) {
        Path path = toPath(keys);
        try(FSDataInputStream in = fs.open(path)) {
            return in;
        } catch (IOException e) {
            throw StoreException.readFailed(toString(), keys, e);
        }
    }

    @Override
    public long getSize(String[] keys) {
        Path path = toPath(keys);

        try {
            return fs.getFileStatus(path).getLen();
        } catch (IOException e) {
            throw StoreException.readFailed(toString(), keys, e);
        }
    }

    @Override
    public String toString() {
        return root.toString();
    }

    private Path toPath(String[] keys){
        Path p = root;
        for (String s: keys) {
            p = new Path(p, s);
        }
        return p;
    }
}
