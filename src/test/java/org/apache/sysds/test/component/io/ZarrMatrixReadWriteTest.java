/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysds.test.component.io;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.sysds.runtime.io.FileFormatPropertiesZarr;
import org.apache.sysds.runtime.io.ReaderZarr;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.junit.BeforeClass;
import org.junit.Test;

import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.DataType;

public class ZarrMatrixReadWriteTest {
    private static final String TEST_DIR = "target/testTemp/component/io/ZarrMatrixReadWriteTest/";
    private static final String STORE_PATH = TEST_DIR + "m2d.zarr";
    private static final String DATASET = new FileFormatPropertiesZarr().getDatasetName();

    //250 x 40
    private static final int ROWS = 1000;
    private static final int COLS = 2;

    @BeforeClass
    public static void setUpClass() throws Exception {
        FileUtils.deleteQuietly(new File(TEST_DIR));

        //data is jsut the index
        double[] vals = new double[ROWS * COLS];
        for (int i = 0; i < vals.length; i++)
            vals[i] = i;

        //create new array
        Array array = Array.create(
                new StoreHandle(
                        new FilesystemStore(STORE_PATH),
                        DATASET
                ),
                Array.metadataBuilder()
                        .withShape(ROWS, COLS)
                        .withDataType(DataType.FLOAT64)
                        .withChunkShape(100, 20)
                        .withFillValue(0.0)
                        .withCodecs(c -> c.withBytes("LITTLE"))
                        .build()
        );

        //write index as data for each datapoint
        array.write(
                new long[]{0, 0},
                ucar.ma2.Array.factory(ucar.ma2.DataType.DOUBLE, new int[]{ROWS, COLS}, vals)
        );
    }

    @Test
    public void read2dDouble() throws Exception {
        MatrixBlock ret = new ReaderZarr(new FileFormatPropertiesZarr())
                .readMatrixFromHDFS(STORE_PATH, ROWS, COLS, 1000, -1);

        assertEquals(ROWS, ret.getNumRows());
        assertEquals(COLS, ret.getNumColumns());
        //first entry should be zero
        assertEquals(0, ret.get(0, 0), 0);
        //"random" access
        assertEquals(123 * COLS + 17, ret.get(123, 17), 0);
        //last elem
        assertEquals(ROWS * COLS - 1, ret.get(ROWS - 1, COLS - 1), 0);
    }
}
