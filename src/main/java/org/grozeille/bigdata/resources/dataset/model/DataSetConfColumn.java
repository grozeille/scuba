package org.grozeille.bigdata.resources.dataset.model;

import lombok.Data;

@Data
public class DataSetConfColumn {

    private String name;

    private String type;

    private String description;

    private String newName;

    private String newType;

    private String newDescription;

    private Boolean selected;

    private Boolean isCalculated;

    private String formula;
}
