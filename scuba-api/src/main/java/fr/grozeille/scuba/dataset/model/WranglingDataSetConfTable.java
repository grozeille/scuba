package fr.grozeille.scuba.dataset.model;

import lombok.Data;

@Data
public class WranglingDataSetConfTable {

    private String database;

    private String table;

    private String path;

    private String comment;

    private String dataDomainOwner;

    private String[] tags;

    private String format;

    private Boolean primary;

    private WranglingDataSetConfColumn[] columns;
}
