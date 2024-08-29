package ch.oepfelbaum.example_join.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Position {
    private int id;
    private int assetId;
    private String assetName;
    private String name;
    private Long timestamp;

    @Override
    public String toString() {
        return "{" +
                "id=" + id +
                ", assetId=" + assetId +
                ", assetName=" + assetName +
                ", name='" + name + '\'' +
                '}';
    }
}
