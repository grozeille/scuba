package fr.grozeille.scuba.dataset.model;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class HiveData {
    private List<Map<String, Object>> data;
}
