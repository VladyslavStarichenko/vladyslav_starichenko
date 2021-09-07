package joins.impl;

import joins.JoinOperation;
import models.DataRow;
import models.JoinedDataRow;

import java.util.*;


public class LeftJoinOperation<K, V1, V2> implements JoinOperation<DataRow<K, V1>, DataRow<K, V2>, JoinedDataRow<K, V1, V2>> {
    @Override
    public Collection<JoinedDataRow<K, V1, V2>> join(Collection<DataRow<K, V1>> leftCollection, Collection<DataRow<K, V2>> rightCollection) {

        Collection<JoinedDataRow<K, V1, V2>> result = new ArrayList<>();
        List<DataRow<K, V1>> checkList = new ArrayList<>();

        leftCollection.forEach(left -> rightCollection.stream()
                .filter(right -> Objects.equals(left.getKey(), right.getKey()))
                .forEach(right -> {
                    result.add(new JoinedDataRow<>(left.getKey(), left.getValue(), right.getValue()));
                    checkList.add(left);
                }));

        leftCollection.stream()
                .filter(left -> !checkList.contains(left))
                .map(left -> new JoinedDataRow<K, V1, V2>(left.getKey(), left.getValue(), null))
                .forEach(result::add);

        return result;
    }
}
