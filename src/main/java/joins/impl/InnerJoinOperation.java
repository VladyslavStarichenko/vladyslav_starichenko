package joins.impl;

import joins.JoinOperation;
import models.DataRow;
import models.JoinedDataRow;

import java.util.ArrayList;
import java.util.Collection;



public class InnerJoinOperation<K, V1, V2> implements JoinOperation<DataRow<K,V1>,DataRow<K,V2>, JoinedDataRow<K, V1, V2>> {
    @Override
    public Collection<JoinedDataRow<K, V1, V2>> join(Collection<DataRow<K, V1>> leftCollection, Collection<DataRow<K, V2>> rightCollection) {
        Collection<JoinedDataRow<K, V1, V2>> result = new ArrayList<>();
        leftCollection.forEach(left ->
                rightCollection.stream()
                .filter(right -> left.getKey().equals(right.getKey()))
                .map(right -> new JoinedDataRow<>(left.getKey(), left.getValue(), right.getValue()))
                .forEach(result::add));
        return result;
    }

}
