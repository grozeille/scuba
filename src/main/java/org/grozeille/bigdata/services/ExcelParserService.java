package org.grozeille.bigdata.services;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.grozeille.bigdata.resources.hive.model.HiveData;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@Service
public class ExcelParserService {

    public String[] sheets(MultipartFile file) throws Exception {

        String[] result = new String[0];

        if(file.getOriginalFilename().endsWith("xlsx") || file.getOriginalFilename().endsWith("xlsm") || file.getOriginalFilename().endsWith("xlsb")){
            OPCPackage pkg = OPCPackage.open(file.getInputStream());
            XSSFWorkbook workbook = new XSSFWorkbook(pkg);
            int nbOfSheets = workbook.getNumberOfSheets();
            result = new String[nbOfSheets];
            for(int cpt = 0; cpt < nbOfSheets; cpt++){
                result[cpt] = workbook.getSheetName(cpt);
            }
        }
        else if(file.getOriginalFilename().endsWith("xls")){
            HSSFWorkbook workbook = new HSSFWorkbook(file.getInputStream());
            int nbOfSheets = workbook.getNumberOfSheets();
            result = new String[nbOfSheets];
            for(int cpt = 0; cpt < nbOfSheets; cpt++){
                result[cpt] = workbook.getSheetName(cpt);
            }
        }

        return result;
    }

    public HiveData data(MultipartFile file, String sheet, Boolean firstLineHeader, Long limit) throws Exception {

        if(limit == null){
            limit = 0l;
        }

        HiveData result = new HiveData();
        result.setData(new ArrayList<>());

        if(file.getOriginalFilename().endsWith("xlsx") || file.getOriginalFilename().endsWith("xlsm") || file.getOriginalFilename().endsWith("xlsb")){
            OPCPackage pkg = OPCPackage.open(file.getInputStream());
            XSSFWorkbook workbook = new XSSFWorkbook(pkg);
            XSSFSheet excelSheet = workbook.getSheet(sheet);
            if(excelSheet == null){
                excelSheet = workbook.getSheetAt(0);
            }

            ArrayList<String> headers = new ArrayList<>();

            int cptRow = 0;
            for(Row row : excelSheet){
                XSSFRow xssfRow = (XSSFRow)row;

                if(cptRow == 0 && firstLineHeader){
                    // considered as header
                    for(Cell cell : xssfRow){
                        XSSFCell xssfcell = (XSSFCell)cell;
                        headers.add(xssfcell.getStringCellValue());
                    }

                } else {
                    Map<String, Object> resultRow = new LinkedHashMap<>();
                    int cptColumn = 0;
                    for(Cell cell : xssfRow){
                        XSSFCell xssfcell = (XSSFCell)cell;
                        String header = "";
                        if(cptColumn < headers.size()){
                            header = headers.get(cptColumn);
                        }
                        else {
                            header = "col_"+cptColumn;
                        }
                        resultRow.put(header, xssfcell.getStringCellValue());
                        cptColumn++;
                    }
                    result.getData().add(resultRow);
                }
                cptRow++;

                if(limit > 0 && cptRow >= limit){
                    break;
                }
            }
        }
        else if(file.getOriginalFilename().endsWith("xls")){
            HSSFWorkbook workbook = new HSSFWorkbook(file.getInputStream());
            HSSFSheet excelSheet = workbook.getSheet(sheet);
            if(excelSheet == null){
                excelSheet = workbook.getSheetAt(0);
            }

            ArrayList<String> headers = new ArrayList<>();

            int cptRow = 0;
            for(Row row : excelSheet){
                HSSFRow hssfRow = (HSSFRow)row;

                if(cptRow == 0 & firstLineHeader){
                    // considered as header
                    for(Cell cell : hssfRow){
                        HSSFCell hssfCell = (HSSFCell)cell;
                        headers.add(hssfCell.getStringCellValue());
                    }

                } else {
                    Map<String, Object> resultRow = new LinkedHashMap<>();
                    int cptColumn = 0;
                    for(Cell cell : hssfRow){
                        HSSFCell hssfCell = (HSSFCell)cell;
                        String header = "";
                        if(cptColumn < headers.size()){
                            header = headers.get(cptColumn);
                        }
                        else {
                            header = "col_"+cptColumn;
                        }
                        resultRow.put(header, hssfCell.getStringCellValue());
                        cptColumn++;
                    }
                    result.getData().add(resultRow);
                }
                cptRow++;

                if(limit > 0 && cptRow >= limit){
                    break;
                }
            }
        }

        return result;
    }
}
