package org.grozeille.bigdata.resources.userdataset.model;

import lombok.Data;

@Data
public class UserDataSetFilterCondition {
    private String database;
    private String table;
    private String column;

    private String condition;

    private String data;
}
