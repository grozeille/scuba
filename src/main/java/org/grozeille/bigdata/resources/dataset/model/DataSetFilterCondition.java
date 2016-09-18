package org.grozeille.bigdata.resources.dataset.model;

import lombok.Data;

@Data
public class DataSetFilterCondition  {
    private String database;
    private String table;
    private String column;

    private String condition;

    private String data;
}
