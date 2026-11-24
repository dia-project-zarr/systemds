package org.apache.sysds.runtime.io;

public class FileFormatPropertiesZarr extends FileFormatProperties {

    private String datasetName;

    public FileFormatPropertiesZarr() {
        this.datasetName = "systemdszarr";
    }

    public String getDatasetName() {
        return this.datasetName;
    }
}
