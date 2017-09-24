package fr.grozeille.scuba.dataset.web.dto;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class DataSetData {
    private List<Map<String, Object>> data;
}
