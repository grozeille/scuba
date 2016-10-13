package org.grozeille.bigdata.services;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;

@Service
public class HdfsService {

    public static final String TEMP_ORIGINAL_PREFIX = "_datalaketoolbox_original";

    public String write(InputStream inputStream, String fileName, String tablePath) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path fullTablePath = new Path(tablePath);
        Path basePath = fullTablePath.getParent();
        Path originalFolderPath = new Path(basePath, TEMP_ORIGINAL_PREFIX+"_"+fullTablePath.getName());
        Path filePath = new Path(originalFolderPath, fileName);

        if(!fs.exists(originalFolderPath)){
            fs.mkdirs(originalFolderPath);
        }

        try(FSDataOutputStream ous = fs.create(filePath)) {
            IOUtils.copy(inputStream, ous);
        }

        return filePath.toString();
    }

    public InputStream read(String path) throws IOException {
        Configuration conf = new Configuration();
        Path filePath = new Path(path);

        FileSystem fs = FileSystem.get(conf);
        return fs.open(filePath);

    }
}
