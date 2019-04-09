package cn.kungreat.elasticsearch.domain;

import lombok.Getter;
import lombok.Setter;

import java.util.Date;

@Setter
@Getter
public class One {
    private Integer id;
    private String name;
    private Integer age;
    private Date birthday;
    private String context;
    private String title;
}
