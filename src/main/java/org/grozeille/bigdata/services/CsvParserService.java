package org.grozeille.bigdata.services;

import com.google.common.base.Strings;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.grozeille.bigdata.resources.hive.model.HiveData;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStreamReader;
import java.util.*;

@Service
public class CsvParserService {

    public HiveData data(MultipartFile file, Character separator, Character textQualifier, boolean firstLineHeader, Long limit) throws Exception {
        CsvParserSettings settings = new CsvParserSettings();
        if(textQualifier != null) {
            settings.getFormat().setQuote(textQualifier);
        }
        else {
            settings.getFormat().setQuote('\0');
        }
        settings.getFormat().setDelimiter(separator);

        CsvParser parser = new CsvParser(settings);
        parser.beginParsing(new InputStreamReader(file.getInputStream()));

        if(limit == null){
            limit = 0l;
        }

        HiveData result = new HiveData();
        result.setData(new ArrayList<>());

        ArrayList<String> headers = new ArrayList<>();

        String[] nextLine;
        int cptRow = 0;
        while ((nextLine = parser.parseNext()) != null) {
            if (nextLine != null) {
                if(cptRow == 0 && firstLineHeader){
                    for(int cptColumn = 0; cptColumn < nextLine.length; cptColumn++){
                        String header = nextLine[cptColumn];
                        if(Strings.isNullOrEmpty(header)){
                            header = "col_"+cptColumn;
                        }
                        headers.add(header);
                    }
                }
                else {
                    Map<String, Object> resultRow = new LinkedHashMap<>();
                    for(int cptColumn = 0; cptColumn < nextLine.length; cptColumn++){
                        String header = "";
                        if(cptColumn < headers.size()){
                            header = headers.get(cptColumn);
                        }
                        else {
                            header = "col_"+cptColumn;
                        }
                        resultRow.put(header, nextLine[cptColumn]);
                    }

                    result.getData().add(resultRow);
                }

                cptRow++;
            }

            if(limit > 0 && cptRow >= limit){
                break;
            }
        }

        parser.stopParsing();

        return result;
    }

    public String[] write(MultipartFile file, Character separator, Character textQualifier, boolean firstLineHeader, String path) throws Exception {

        List<String> columns = new ArrayList<>();

        Configuration conf = new Configuration();
        Path orcPath = new Path(path+"/data.orc");

        // TODO: do better, create versions
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(path), true);

        CsvParserSettings settings = new CsvParserSettings();
        if(textQualifier != null) {
            settings.getFormat().setQuote(textQualifier);
        }
        else {
            settings.getFormat().setQuote('\0');
        }
        settings.getFormat().setDelimiter(separator);

        CsvParser parser = new CsvParser(settings);
        parser.beginParsing(new InputStreamReader(file.getInputStream()));

        Writer writer = null;


        String[] nextLine;
        int cptRow = 0;
        while ((nextLine = parser.parseNext()) != null) {
            if (nextLine != null) {
                if(cptRow == 0 && firstLineHeader){
                    TypeDescription schema = TypeDescription.createStruct();
                    for(int cptColumn = 0; cptColumn < nextLine.length; cptColumn++){
                        String header = nextLine[cptColumn];
                        if(Strings.isNullOrEmpty(header)){
                            header = "col_"+cptColumn;
                        }
                        schema.addField(header, TypeDescription.createString());
                        columns.add(header);

                        String typeStr = schema.toString();
                        TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeStr);
                        ObjectInspector inspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
                        writer = OrcFile.createWriter(orcPath, OrcFile.writerOptions(conf).inspector(inspector).stripeSize(100000).bufferSize(10000));
                    }
                }
                else {
                    if(cptRow == 0){
                        TypeDescription schema = TypeDescription.createStruct();
                        for(int cptColumn = 0; cptColumn < nextLine.length; cptColumn++){
                            schema.addField("col_"+cptColumn, TypeDescription.createString());
                            columns.add("col_"+cptColumn);
                        }

                        String typeStr = schema.toString();
                        StructTypeInfo typeInfo = (StructTypeInfo) TypeInfoUtils.getTypeInfoFromTypeString(typeStr);
                        ObjectInspector inspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);

                        writer = OrcFile.createWriter(orcPath, OrcFile.writerOptions(conf).inspector(inspector).stripeSize(100000).bufferSize(10000));
                    }

                    writer.addRow(nextLine);
                }

                cptRow++;
            }
        }

        parser.stopParsing();

        writer.close();

        return columns.toArray(new String[0]);
    }
}
