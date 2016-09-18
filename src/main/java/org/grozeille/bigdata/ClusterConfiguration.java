package org.grozeille.bigdata;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "datalake.toolbox")
public class ClusterConfiguration {

}
