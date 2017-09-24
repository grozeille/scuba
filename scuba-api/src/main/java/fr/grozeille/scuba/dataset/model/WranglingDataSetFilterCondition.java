package fr.grozeille.scuba.dataset.model;

import lombok.Data;

@Data
public class WranglingDataSetFilterCondition {
    private String database;
    private String table;
    private String column;

    private String condition;

    private String data;
}
