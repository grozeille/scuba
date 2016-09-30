package org.grozeille.bigdata.resources.excel.model;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class FileData {
    private List<Map<String, Object>> data;
}
