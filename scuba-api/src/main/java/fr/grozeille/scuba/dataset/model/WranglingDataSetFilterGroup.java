package fr.grozeille.scuba.dataset.model;

import lombok.Data;

@Data
public class WranglingDataSetFilterGroup {
    private String operator;

    private WranglingDataSetFilterCondition[] conditions;

    private WranglingDataSetFilterGroup[] groups;
}
