package fr.grozeille.scuba.dataset.model;

import lombok.Data;

import java.util.Map;

@Data
public class HiveTable {
    private String database;

    private String table;

    private String path;

    private String comment;

    private String creator;

    private String[] tags;

    private String format;

    private String dataSetType;

    private String dataSetConfiguration;

    private Boolean temporary;

    private Map<String, String> otherProperties;

    private HiveColumn[] columns;
}
