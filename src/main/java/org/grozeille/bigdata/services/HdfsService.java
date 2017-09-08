package org.grozeille.bigdata.services;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;

@Service
public class HdfsService {

    public static final String TEMP_ORIGINAL_PREFIX = "_datalaketoolbox_original";

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class HdfsFileInfo {
        private String filePath;

        private int size;
    }

    @Autowired
    private FileSystem fs;

    public HdfsFileInfo write(InputStream inputStream, String fileName, String tablePath) throws IOException {
        // TODO use project working path

        Path fullTablePath = new Path(tablePath);
        Path basePath = fullTablePath.getParent();
        Path originalFolderPath = new Path(basePath, TEMP_ORIGINAL_PREFIX+"_"+fullTablePath.getName());
        Path filePath = new Path(originalFolderPath, fileName);

        if(!fs.exists(originalFolderPath)){
            fs.mkdirs(originalFolderPath);
        }

        int size = 0;
        try(FSDataOutputStream ous = fs.create(filePath)) {
            size = IOUtils.copy(inputStream, ous);
        }

        return new HdfsFileInfo(filePath.toString(), size);
    }

    public InputStream read(String path) throws IOException {
        Path filePath = new Path(path);
        return fs.open(filePath);
    }

    public void delete(String path) throws IOException {
        fs.delete(new Path(path), true);
    }
}
