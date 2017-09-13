package org.grozeille.bigdata.dataset.model;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class HiveData {
    private List<Map<String, Object>> data;
}
