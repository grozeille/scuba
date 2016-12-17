package org.grozeille.bigdata.resources.userdataset.model;

import lombok.Data;

@Data
public class UserDataSetConfLink {

    public static final String TYPE_INNER = "inner";

    public static final String TYPE_OUTER = "outer";

    private UserDataSetConfLinkTable left;

    private UserDataSetConfLinkTable right;

    private UserDataSetConfLinkColumn[] columns;

    private String type;
}
