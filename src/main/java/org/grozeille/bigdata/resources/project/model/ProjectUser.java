package org.grozeille.bigdata.resources.project.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.grozeille.bigdata.resources.user.model.User;

import javax.persistence.*;
import java.util.List;

@Entity
@Table(name = "USER")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProjectUser {
    @Id
    @Column(name = "LOGIN")
    private String login;
}
