package org.grozeille.bigdata.resources.userdataset.model;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "DATASET")
@Data
public class UserDataSet {

    @Id
    @Column(name = "ID")
    private String id;

    @Column(name = "JSONCONF")
    private String jsonConf;
}
