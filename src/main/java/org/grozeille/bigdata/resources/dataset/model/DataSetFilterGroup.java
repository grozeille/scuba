package org.grozeille.bigdata.resources.dataset.model;

import lombok.Data;

@Data
public class DataSetFilterGroup  {
    private String operator;

    private DataSetFilterCondition[] conditions;

    private DataSetFilterGroup[] groups;
}
