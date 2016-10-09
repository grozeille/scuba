package org.grozeille.bigdata.services;

import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.TypeDescription;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.grozeille.bigdata.resources.hive.model.HiveData;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class RawParserService {
    public HiveData data(MultipartFile file, Long limit) throws Exception {
        HiveData result = new HiveData();
        result.setData(new ArrayList<>());
        final String column = "raw";

        try (BufferedReader br = new BufferedReader(new InputStreamReader(file.getInputStream()))) {
            String line;
            while ((line = br.readLine()) != null) {
                Map<String, Object> row = new HashMap<>(1);
                row.put(column, line);
                result.getData().add(row);
            }
        }
        return result;
    }

    public String[] write(MultipartFile file, String path) throws Exception {
        List<String> columns = new ArrayList<>();

        Configuration conf = new Configuration();
        Path orcPath = new Path(path+"/data.orc");

        // TODO: do better, create versions
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(path), true);

        final String column = "raw";
        TypeDescription schema = TypeDescription.createStruct();
        schema.addField(column, TypeDescription.createString());
        columns.add(column);

        String typeStr = schema.toString();
        TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeStr);
        ObjectInspector inspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
        Writer writer = OrcFile.createWriter(orcPath, OrcFile.writerOptions(conf).inspector(inspector).stripeSize(100000).bufferSize(10000));

        try (BufferedReader br = new BufferedReader(new InputStreamReader(file.getInputStream()))) {
            String line;
            while ((line = br.readLine()) != null) {
                writer.addRow(line);
            }
        }

        writer.close();

        return columns.toArray(new String[0]);
    }
}
