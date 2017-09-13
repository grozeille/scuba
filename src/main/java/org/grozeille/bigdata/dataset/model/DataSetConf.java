package org.grozeille.bigdata.dataset.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DataSetConf {
    private String database;

    private String table;

    private String comment;

    private String[] tags;

}
