package org.grozeille.bigdata.dataset.model;

import lombok.Data;

@Data
public class WranglingDataSetConfLink {

    public static final String TYPE_INNER = "inner";

    public static final String TYPE_OUTER = "outer";

    private WranglingDataSetConfLinkTable left;

    private WranglingDataSetConfLinkTable right;

    private WranglingDataSetConfLinkColumn[] columns;

    private String type;
}
