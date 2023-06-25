/*
 * Copyright (c) 2017 Data and Web Science Group, University of Mannheim, Germany (http://dws.informatik.uni-mannheim.de/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package de.uni_mannheim.informatik.dws.winter.usecase.web_movie;

import de.uni_mannheim.informatik.dws.winter.matching.algorithms.SimilarityFloodingAlgorithm;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.SimilarityFloodingSchema;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.HashedDataSet;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.CSVRecordReader;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.ColumnType;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.utils.WinterLogManager;
import de.uni_mannheim.informatik.dws.winter.webtables.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.detectors.tabletypeclassifier.TypeClassifier;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.slf4j.Logger;

/**
 * @author Robin Schumacher (info@robin-schumacher.com
 */
public class Movies_SimilarityFloodingSchemaMatching {

    private static final Logger logger = WinterLogManager.activateLogger("default");

    public static void main(String[] args) throws Exception {
        HashMap<String, String> goldStandard = new HashMap<>();
        goldStandard.put("title", "rdf-schema#label");
        goldStandard.put("year", "releaseDate");
        goldStandard.put("director", "director");
        goldStandard.put("writer", "writer");
        goldStandard.put("length", "duration");

        // load data
        DataSet<Record, Attribute> data1 = new HashedDataSet<>();
        new CSVRecordReader(0).loadFromCSV(new File("usecase/webtable_movie/input/Film.csv"), data1);
        DataSet<Record, Attribute> data2 = new HashedDataSet<>();
        new CSVRecordReader(0).loadFromCSV(new File("usecase/webtable_movie/input/50245608_0_871275842592178099.csv"), data2);

        // Initialize Matching Engine

        SimilarityFloodingSchema schema1 = getSimilarityFloodingSchema(data1);
        SimilarityFloodingSchema schema2 = getSimilarityFloodingSchema(data2);

        SimilarityFloodingAlgorithm similarityFloodingAlgorithm = new SimilarityFloodingAlgorithm(schema2, schema1);
        similarityFloodingAlgorithm.run();
        Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>> correspondences = similarityFloodingAlgorithm.getResult();

        // print results
        for (Correspondence<MatchableTableColumn, MatchableTableColumn> cor : correspondences.get()) {
            logger.info(String.format("[%s]'%s' <-> [%s]'%s' (%.4f)",
                cor.getFirstRecord().getIdentifier(),
                cor.getFirstRecord().getHeader(),
                cor.getSecondRecord().getIdentifier(),
                cor.getSecondRecord().getHeader(),
                cor.getSimilarityScore()));
        }
    }

    private static SimilarityFloodingSchema getSimilarityFloodingSchema(DataSet<Record, Attribute> data1) {
        List<MatchableTableColumn> schema1List = new ArrayList<>();
        SimilarityFloodingSchema schema1 = new SimilarityFloodingSchema("Film", schema1List);

        TypeClassifier typeClassifier = new TypeClassifier();
        typeClassifier.initialize();

        int count = 0;
        for (Attribute attribute : data1.getSchema().get()) {

            List<String> typeList = new ArrayList<>();
            List<Record> list = new ArrayList<>(data1.get());

            for (Record record : list) {
                typeList.add(record.getValue(attribute));
            }

            String[] ar = new String[100];
            int j = 0;
            for (int i = 0; j < 100 && i < typeList.size(); i++) {
                if (!typeList.get(i).equals("NULL")) {
                    ar[j] = typeList.get(i);
                    j++;
                }
            }
            ColumnType type = typeClassifier.detectTypeForColumn(ar, attribute.getName());

            DataType ty = type.getType();
            if (ty.equals(DataType.unit)) {
                ty = DataType.numeric;
            }
            schema1List.add(new MatchableTableColumn(0, count++, attribute.getName(), ty));
        }
        return schema1;
    }

}
