package org.grozeille.bigdata.resources.userdataset.model;

import lombok.Data;

@Data
public class UserDataSetFilterGroup {
    private String operator;

    private UserDataSetFilterCondition[] conditions;

    private UserDataSetFilterGroup[] groups;
}
