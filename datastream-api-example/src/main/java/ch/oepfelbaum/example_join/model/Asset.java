package ch.oepfelbaum.example_join.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Asset {
    private int id;
    private String name;
    private Long timestamp;

    @Override
    public String toString() {
        return "{" +
                "id=" + id +
                ", name='" + name + '\'' +
                '}';
    }
}
