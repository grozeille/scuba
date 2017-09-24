package fr.grozeille.scuba.dataset.services;

import fr.grozeille.scuba.dataset.model.HiveData;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.TypeDescription;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class ExcelParserService {

    @Autowired
    private FileSystem fs;

    public String[] sheets(InputStream inputStream, String fileName) throws Exception {

        String[] result = new String[0];

        if(fileName.endsWith("xlsx") || fileName.endsWith("xlsm") || fileName.endsWith("xlsb")){
            OPCPackage pkg = OPCPackage.open(inputStream);
            XSSFWorkbook workbook = new XSSFWorkbook(pkg);
            int nbOfSheets = workbook.getNumberOfSheets();
            result = new String[nbOfSheets];
            for(int cpt = 0; cpt < nbOfSheets; cpt++){
                result[cpt] = workbook.getSheetName(cpt);
            }
        }
        else if(fileName.endsWith("xls")){
            HSSFWorkbook workbook = new HSSFWorkbook(inputStream);
            int nbOfSheets = workbook.getNumberOfSheets();
            result = new String[nbOfSheets];
            for(int cpt = 0; cpt < nbOfSheets; cpt++){
                result[cpt] = workbook.getSheetName(cpt);
            }
        }

        return result;
    }

    public HiveData data(InputStream inputStream, String fileName, String sheet, Boolean firstLineHeader, Long limit) throws Exception {

        if(limit == null){
            limit = 0l;
        }

        HiveData result = new HiveData();
        result.setData(new ArrayList<>());

        Sheet excelSheet = null;

        if(fileName.endsWith("xlsx") || fileName.endsWith("xlsm") || fileName.endsWith("xlsb")){
            OPCPackage pkg = OPCPackage.open(inputStream);
            XSSFWorkbook workbook = new XSSFWorkbook(pkg);
            excelSheet = workbook.getSheet(sheet);
            if(excelSheet == null){
                excelSheet = workbook.getSheetAt(0);
            }
        }
        else if(fileName.endsWith("xls")){
            HSSFWorkbook workbook = new HSSFWorkbook(inputStream);
            excelSheet = workbook.getSheet(sheet);
            if(excelSheet == null){
                excelSheet = workbook.getSheetAt(0);
            }
        }

        ArrayList<String> headers = new ArrayList<>();

        int cptRow = 0;
        for(Row row : excelSheet){

            if(cptRow == 0 && firstLineHeader){
                // considered as header
                for(Cell cell : row){
                    headers.add(cell.getStringCellValue());
                }

            } else {
                Map<String, Object> resultRow = new LinkedHashMap<>();
                int cptColumn = 0;
                for(Cell cell : row){
                    String header = "";
                    if(cptColumn < headers.size()){
                        header = headers.get(cptColumn);
                    }
                    else {
                        header = "col_"+cptColumn;
                    }
                    resultRow.put(header, cell.getStringCellValue());
                    cptColumn++;
                }
                result.getData().add(resultRow);
            }
            cptRow++;

            if(limit > 0 && cptRow >= limit){
                break;
            }
        }

        return result;
    }

    public String[] write(InputStream inputStream, String fileName, String sheet, boolean firstLineHeader, String path) throws Exception {

        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();

        List<String> columns = new ArrayList<>();

        Path orcPath = new Path(path+"/data.orc");

        // TODO: do better, create versions
        fs.delete(new Path(path), true);

        Writer writer = null;

        Sheet excelSheet = null;

        if(fileName.endsWith("xlsx") || fileName.endsWith("xlsm") || fileName.endsWith("xlsb")){
            OPCPackage pkg = OPCPackage.open(inputStream);
            XSSFWorkbook workbook = new XSSFWorkbook(pkg);
            excelSheet = workbook.getSheet(sheet);
            if(excelSheet == null){
                excelSheet = workbook.getSheetAt(0);
            }
        }
        else if(fileName.endsWith("xls")){
            HSSFWorkbook workbook = new HSSFWorkbook(inputStream);
            excelSheet = workbook.getSheet(sheet);
            if(excelSheet == null){
                excelSheet = workbook.getSheetAt(0);
            }
        }

        int cptRow = 0;
        for(Row row : excelSheet){

            if(cptRow == 0 && firstLineHeader){
                TypeDescription schema = TypeDescription.createStruct();
                // considered as header
                for(Cell cell : row){
                    columns.add(cell.getStringCellValue());
                    schema.addField(cell.getStringCellValue(), TypeDescription.createString());
                }
                String typeStr = schema.toString();
                TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeStr);
                ObjectInspector inspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
                writer = OrcFile.createWriter(orcPath, OrcFile.writerOptions(conf).inspector(inspector).stripeSize(100000).bufferSize(10000));

            } else {

                if(cptRow == 0 ){
                    TypeDescription schema = TypeDescription.createStruct();
                    // considered as header
                    int cptColumn = 0;
                    for(Cell cell : row){
                        columns.add("col_"+cptColumn);
                        schema.addField(cell.getStringCellValue(), TypeDescription.createString());
                        cptColumn++;
                    }
                    String typeStr = schema.toString();
                    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeStr);
                    ObjectInspector inspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
                    writer = OrcFile.createWriter(orcPath, OrcFile.writerOptions(conf).inspector(inspector).stripeSize(100000).bufferSize(10000));

                }

                List<String> arrayRow = new ArrayList<>();
                int cptColumn = 0;
                for(Cell cell : row){
                    if(cptColumn >= columns.size()){
                        log.warn("Ignoring column #"+cptColumn+" at line #"+cptRow+" because the line is longer than the first line");
                        break;
                    }
                    else {
                        arrayRow.add(cell.getStringCellValue());
                    }
                }

                writer.addRow(arrayRow.toArray(new String[0]));
            }
            cptRow++;
        }

        writer.close();

        return columns.toArray(new String[0]);
    }
}
