package org.grozeille.bigdata.resources.wranglingdataset.model;

import lombok.Data;

@Data
public class WranglingDataSetFilterGroup {
    private String operator;

    private WranglingDataSetFilterCondition[] conditions;

    private WranglingDataSetFilterGroup[] groups;
}
