package org.grozeille.bigdata.resources.user.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.grozeille.bigdata.resources.user.model.User;

import javax.persistence.*;
import java.util.List;

@Entity
@Table(name = "PROJECT")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserProject {
    @Id
    @Column(name = "ID")
    private String id;

    @Column(name = "NAME")
    private String name;

    @Column(name = "HIVE_DB")
    private String hiveDatabase;

    @Column(name = "HDFS_WORKING_DIR")
    private String hdfsWorkingDirectory;

}
