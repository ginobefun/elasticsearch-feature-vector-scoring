/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ginobefunny.elasticsearch.plugins.scoring;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.ScriptException;

import java.util.Map;

public class FeatureVectorScoringSearchScript extends AbstractSearchScript {

    public static final ESLogger LOGGER = Loggers.getLogger("feature-vector-scoring");

    public static final String SCRIPT_NAME = "feature_vector_scoring_script";

    private static final double DEFAULT_BASE_CONSTANT = 1.0D;

    private static final double DEFAULT_FACTOR_CONSTANT = 1.0D;

    // field in index to store feature vector
    private String field;

    // version of feature vector, if it isn't null, it should match version of index
    private String version;

    // final_score = baseConstant + factorConstant * cos(X, Y)
    private double baseConstant;

    // final_score = baseConstant + factorConstant * cos(X, Y)
    private double factorConstant;

    // input feature vector
    private double[] inputFeatureVector;

    // cos(X, Y) = Σ(Xi * Yi) / ( sqrt(Σ(Xi * Xi)) * sqrt(Σ(Yi * Yi)) )
    // the inputFeatureVectorNorm is sqrt(Σ(Xi * Xi))
    private double inputFeatureVectorNorm;

    public static class ScriptFactory implements NativeScriptFactory {

        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params) throws ScriptException {
            return new FeatureVectorScoringSearchScript(params);
        }

        @Override
        public boolean needsScores() {
            return false;
        }
    }

    private FeatureVectorScoringSearchScript(Map<String, Object> params) throws ScriptException {
        this.field = (String) params.get("field");
        String inputFeatureVectorStr = (String) params.get("inputFeatureVector");
        if (this.field == null || inputFeatureVectorStr == null || inputFeatureVectorStr.trim().length() == 0) {
            throw new ScriptException("Initialize script " + SCRIPT_NAME + " failed!");
        }

        this.version = (String) params.get("version");
        this.baseConstant = params.get("baseConstant") != null ? Double.parseDouble(params.get("baseConstant").toString()) : DEFAULT_BASE_CONSTANT;
        this.factorConstant = params.get("factorConstant") != null ? Double.parseDouble(params.get("factorConstant").toString()) : DEFAULT_FACTOR_CONSTANT;

        String[] inputFeatureVectorArr = inputFeatureVectorStr.split(",");
        int dimension = inputFeatureVectorArr.length;
        double sumOfSquare = 0.0D;
        this.inputFeatureVector = new double[dimension];
        double temp;
        for (int index = 0; index < dimension; index++) {
            temp = Double.parseDouble(inputFeatureVectorArr[index].trim());
            this.inputFeatureVector[index] = temp;
            sumOfSquare += temp * temp;
        }

        this.inputFeatureVectorNorm = Math.sqrt(sumOfSquare);
        LOGGER.debug("FeatureVectorScoringSearchScript.init, version:{}, norm:{}, baseConstant:{}, factorConstant:{}."
                , this.version, this.inputFeatureVectorNorm, this.baseConstant, this.factorConstant);
    }

    @Override
    public Object run() {
        if (this.inputFeatureVectorNorm == 0) {
            return this.baseConstant;
        }

        if (!doc().containsKey(this.field) || doc().get(this.field) == null) {
            LOGGER.error("cannot find field {}.", field);
            return this.baseConstant;
        }

        String docFeatureVectorStr = ((ScriptDocValues.Strings) doc().get(this.field)).getValue();
        return calculateScore(docFeatureVectorStr);
    }

    public double calculateScore(String docFeatureVectorStr) {
        // 1. check docFeatureVector
        if (docFeatureVectorStr == null) {
            return this.baseConstant;
        }

        docFeatureVectorStr = docFeatureVectorStr.trim();
        if (docFeatureVectorStr.isEmpty()) {
            return this.baseConstant;
        }

        // 2. check version and get feature vector array of document
        String[] docFeatureVectorArr;
        if (this.version != null) {
            String versionPrefix = version + "|";
            if (!docFeatureVectorStr.startsWith(versionPrefix)) {
                return this.baseConstant;
            }

            docFeatureVectorArr = docFeatureVectorStr.substring(versionPrefix.length()).split(",");
        } else {
            docFeatureVectorArr = docFeatureVectorStr.split(",");
        }

        // 3. check the dimension of input and document
        int dimension = this.inputFeatureVector.length;
        if (docFeatureVectorArr == null || docFeatureVectorArr.length != dimension) {
            return this.baseConstant;
        }

        // 4. calculate the relevance score of the two feature vector
        double sumOfSquare = 0.0D;
        double sumOfProduct = 0.0D;
        double tempValueInDouble;
        for (int i = 0; i < dimension; i++) {
            tempValueInDouble = Double.parseDouble(docFeatureVectorArr[i].trim());
            sumOfProduct += tempValueInDouble * this.inputFeatureVector[i];
            sumOfSquare += tempValueInDouble * tempValueInDouble;
        }

        if (sumOfSquare == 0) {
            return this.baseConstant;
        }

        double cosScore = sumOfProduct / (Math.sqrt(sumOfSquare) * inputFeatureVectorNorm);
        return this.baseConstant + this.factorConstant * cosScore;
    }
}