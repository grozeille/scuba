package fr.grozeille.scuba;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "datalake.toolbox")
public class ClusterConfiguration {
    private ClusterSolrConfiguration solr;
    private String workingPath;

    private String adminToken;
}
