package org.grozeille.bigdata.resources.wranglingdataset.model;

import lombok.Data;

@Data
public class WranglingDataSetFilterCondition {
    private String database;
    private String table;
    private String column;

    private String condition;

    private String data;
}
