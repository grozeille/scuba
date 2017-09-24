package fr.grozeille.scuba;

import lombok.Data;

@Data
public class ClusterSolrConfiguration {
    private boolean embedded;
    private String home;
    private String zkUrl;
}
