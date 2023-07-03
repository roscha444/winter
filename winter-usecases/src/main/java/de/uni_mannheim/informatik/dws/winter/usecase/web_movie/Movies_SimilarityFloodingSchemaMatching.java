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
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.FixpointFormula;
import de.uni_mannheim.informatik.dws.winter.matching.rules.comparators.Comparator;
import de.uni_mannheim.informatik.dws.winter.matching.rules.comparators.ComparatorLogger;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.HashedDataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.CSVRecordReader;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Record;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.ColumnType;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.similarity.string.LevenshteinSimilarity;
import de.uni_mannheim.informatik.dws.winter.utils.WinterLogManager;
import de.uni_mannheim.informatik.dws.winter.webtables.SFMatchable;
import de.uni_mannheim.informatik.dws.winter.webtables.detectors.tabletypeclassifier.TypeClassifier;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;

/**
 * @author Robin Schumacher (info@robin-schumacher.com)
 */
public class Movies_SimilarityFloodingSchemaMatching {

    private static final Logger logger = WinterLogManager.activateLogger("default");

    public static void main(String[] args) throws Exception {
        // load data
        DataSet<Record, Attribute> data1 = new HashedDataSet<>();
        new CSVRecordReader(0).loadFromCSV(new File("usecase/webtable_movie/input/Film.csv"), data1);
        DataSet<Record, Attribute> data2 = new HashedDataSet<>();
        new CSVRecordReader(0).loadFromCSV(new File("usecase/webtable_movie/input/50245608_0_871275842592178099.csv"), data2);

        // Initialize Matching Engine

        SimilarityFloodingAlgorithm<SFTestMatchable, SFTestMatchable> sf = new SimilarityFloodingAlgorithm<>(getSimilarityFloodingSchema(data2), getSimilarityFloodingSchema(data1),
            new SFComparatorLevenshtein(), FixpointFormula.A);
        sf.setRemoveOid(true);
        sf.setDefaultSim(Double.MIN_VALUE);
        sf.setMinSim(0.05);
        sf.run();
        Processable<Correspondence<SFTestMatchable, SFTestMatchable>> correspondences = sf.getResult();

        // print results
        for (Correspondence<SFTestMatchable, SFTestMatchable> cor : correspondences.get()) {
            logger.info(String.format("[%s]'%s' <-> [%s]'%s' (%.4f)",
                cor.getFirstRecord().getIdentifier(),
                cor.getFirstRecord().getValue(),
                cor.getSecondRecord().getIdentifier(),
                cor.getSecondRecord().getValue(),
                cor.getSimilarityScore()));
        }
    }

    private static List<SFTestMatchable> getSimilarityFloodingSchema(DataSet<Record, Attribute> data1) {
        List<SFTestMatchable> schema1List = new ArrayList<>();

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
            schema1List.add(new SFTestMatchable(ty, attribute.getName(), attribute.getName()));
        }
        return schema1List;
    }

    public static class SFComparatorLevenshtein implements Comparator<SFTestMatchable, SFTestMatchable> {

        private static final long serialVersionUID = 1L;
        private final LevenshteinSimilarity similarity = new LevenshteinSimilarity();
        private ComparatorLogger comparisonLog;


        @Override
        public double compare(SFTestMatchable record1, SFTestMatchable record2, Correspondence<SFTestMatchable, Matchable> schemaCorrespondence) {
            return similarity.calculate(record1.getValue(), record2.getValue());
        }

        @Override
        public ComparatorLogger getComparisonLog() {
            return this.comparisonLog;
        }

        @Override
        public void setComparisonLog(ComparatorLogger comparatorLog) {
            this.comparisonLog = comparatorLog;
        }

    }

    private static class SFTestMatchable extends SFMatchable {

        private final String id;
        private final String value;

        public SFTestMatchable(DataType type, String id, String value) {
            super(type);
            this.id = id;
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String getIdentifier() {
            return id;
        }

        @Override
        public String getProvenance() {
            return null;
        }
    }

}
