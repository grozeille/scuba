package org.grozeille.bigdata.resources.hive.model;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class HiveData {
    private List<Map<String, Object>> data;
}
