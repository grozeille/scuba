package fr.grozeille.scuba.dataset.services;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;

@Service
public class HdfsService {

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
        Path filePath = computeFilePath(fileName, tablePath);

        int size = 0;
        try(FSDataOutputStream ous = fs.create(filePath)) {
            size = IOUtils.copy(inputStream, ous);
        }

        return new HdfsFileInfo(fs.getFileStatus(filePath).getPath().toString(), size);
    }

    public InputStream read(String path) throws IOException {
        Path filePath = new Path(path);
        return fs.open(filePath);
    }

    public void delete(String path) throws IOException {
        fs.delete(new Path(path), true);
    }

    public HdfsFileInfo copy(String sourceFilePath, String tablePath) throws IOException {
        Path sourceFileHdfsPath = new Path(sourceFilePath);

        Path targetPath = computeFilePath(sourceFileHdfsPath.getName(), tablePath);

        FileUtil.copy(fs, sourceFileHdfsPath, fs, targetPath, false, true, fs.getConf());

        FileStatus fileStatus = fs.getFileStatus(targetPath);

        return new HdfsFileInfo(fileStatus.getPath().toString(), (int) fileStatus.getLen());
    }

    private Path computeFilePath(String fileName, String tablePath) throws IOException {
        Path tableMetaFolder = new Path(tablePath+ ".meta");
        if(!fs.exists(tableMetaFolder)){
            fs.mkdirs(tableMetaFolder);
        }

        return new Path(tableMetaFolder, fileName);
    }

}
