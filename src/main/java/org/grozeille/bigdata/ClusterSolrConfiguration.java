package org.grozeille.bigdata;

import lombok.Data;

@Data
public class ClusterSolrConfiguration {
    private boolean embedded;
    private String home;
    private String zkUrl;
}
