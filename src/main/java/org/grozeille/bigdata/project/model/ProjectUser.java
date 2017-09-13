package org.grozeille.bigdata.project.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

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
