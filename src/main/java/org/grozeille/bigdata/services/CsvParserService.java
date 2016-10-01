package org.grozeille.bigdata.services;

import com.google.common.base.Strings;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
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
}
