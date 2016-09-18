package org.grozeille.bigdata.resources.dataset.model;

import lombok.Data;

@Data
public class DataSetConfLink {

    public static final String TYPE_INNER = "inner";

    public static final String TYPE_OUTER = "outer";

    private DataSetConfLinkTable left;

    private DataSetConfLinkTable right;

    private DataSetConfLinkColumn[] columns;

    private String type;
}
