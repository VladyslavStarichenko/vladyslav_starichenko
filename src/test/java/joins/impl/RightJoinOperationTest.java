package joins.impl;

import models.DataRow;
import models.JoinedDataRow;
import org.junit.jupiter.api.Test;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;


class RightJoinOperationTest {

    @Test
    void rightJoinOperationTest() {

        //given
        RightJoinOperation<Integer, String, String> rightJoinOperation = new RightJoinOperation<>();

        List<DataRow<Integer, String>> leftCollection = Arrays.asList(
                new DataRow<>(0, "Ukraine"),
                new DataRow<>(1, "Germany"),
                new DataRow<>(2, "France"),
                new DataRow<>(5, "Switzerland"),
                new DataRow<>(7, "Spain"));


        List<DataRow<Integer, String>> rightCollection = Arrays.asList(
                new DataRow<>(0, "Kyiv"),
                new DataRow<>(1, "Berlin"),
                new DataRow<>(3, "Budapest"),
                new DataRow<>(5, "Bern"),
                new DataRow<>(0, "Kharkiv"),
                new DataRow<>(10, "Milan"));

        Collection<JoinedDataRow<Integer, String, String>> joinedDataRows = Arrays.asList(
                new JoinedDataRow<>(0, "Ukraine", "Kyiv"),
                new JoinedDataRow<>(0, "Ukraine", "Kharkiv"),
                new JoinedDataRow<>(1, "Germany", "Berlin"),
                new JoinedDataRow<>(5, "Switzerland", "Bern"),
                new JoinedDataRow<>(3, null, "Budapest"),
                new JoinedDataRow<>(10, null, "Milan")
        );

        //when
        Collection<JoinedDataRow<Integer, String, String>> resultCollection = rightJoinOperation.join(leftCollection, rightCollection);
        boolean isResultRight = resultCollection.containsAll(joinedDataRows);

        //then
        assertThat(isResultRight).isTrue();

    }

    @Test
    void emptyIfInitialCollectionIsEmpty(){
        //given
        RightJoinOperation<Integer, String, String> rightJoinOperation = new RightJoinOperation<>();
        List<DataRow<Integer, String>> leftCollection = new ArrayList<>();
        List<DataRow<Integer, String>> rightCollection =  new ArrayList<>();

        //when
        Collection<JoinedDataRow<Integer, String, String>> resultCollection = rightJoinOperation.join(leftCollection, rightCollection);
        boolean isCollectionEmpty = resultCollection.isEmpty();

        //then
        assertThat(isCollectionEmpty).isTrue();
    }

    @Test
    void ifNoMatchWithJoinTable(){
        //given
        RightJoinOperation<Integer, String, String> rightJoinOperation = new RightJoinOperation<>();
        List<DataRow<Integer, String>> rightCollection = Arrays.asList(
                new DataRow<>(0, "Kyiv"),
                new DataRow<>(1, "Berlin"),
                new DataRow<>(3, "Budapest"),
                new DataRow<>(5, "Bern"),
                new DataRow<>(0, "Kharkiv"),
                new DataRow<>(10, "Milan"));
        List<DataRow<Integer, String>> leftCollection =  new ArrayList<>();

        Collection<JoinedDataRow<Integer, String, String>> joinedDataRows = Arrays.asList(
                new JoinedDataRow<>(0, null, "Kyiv"),
                new JoinedDataRow<>(1, null, "Berlin"),
                new JoinedDataRow<>(3, null, "Budapest"),
                new JoinedDataRow<>(5, null, "Bern"),
                new JoinedDataRow<>(0, null, "Kharkiv"),
                new JoinedDataRow<>(10, null, "Milan")
        );

        //when
        Collection<JoinedDataRow<Integer, String, String>> resultCollection = rightJoinOperation.join(leftCollection, rightCollection);
        boolean isCollectionEmpty = resultCollection.isEmpty();
        boolean isResultRight = resultCollection.containsAll(joinedDataRows);
        //then
        assertThat(isCollectionEmpty).isFalse();
        assertThat(isResultRight).isTrue();
    }

}