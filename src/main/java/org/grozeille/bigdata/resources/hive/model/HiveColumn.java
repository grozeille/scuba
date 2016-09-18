package org.grozeille.bigdata.resources.hive.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class HiveColumn {

    private String name;

    private String type;

    private String description;

    private HiveColumnStatistics statistics;
}
