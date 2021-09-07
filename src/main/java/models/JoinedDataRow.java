package models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class JoinedDataRow<K, V1, V2>{

    private K key;
    private V1 value1;
    private V2 value2;

}
