package org.nerd.kid.extractor;

import com.google.common.collect.Sets;
import org.nerd.kid.data.WikidataElement;
import org.nerd.kid.data.WikidataElementInfos;
import org.nerd.kid.extractor.wikidata.WikidataFetcherWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/*
 * extract features (properties and values) of WikidataId directly from Wikidata or Nerd knowledge base
 **/
public class FeatureDataExtractor {
    private static final Logger LOGGER = LoggerFactory.getLogger(FeatureDataExtractor.class);

    private WikidataFetcherWrapper wikidataFetcherWrapper = null;

    // for reading feature pattern in feature mapper files in '/resources' directory
    private FeatureFileExtractor featureFileExtractor = new FeatureFileExtractor();
    private List<String> featuresNoValueList = featureFileExtractor.loadFeaturesNoValue();
    private List<String> featuresList = featureFileExtractor.loadFeatures();

    public FeatureDataExtractor() {
    }

    public FeatureDataExtractor(WikidataFetcherWrapper wikidataFetcherWrapper) {
        this.wikidataFetcherWrapper = wikidataFetcherWrapper;
    }

    public int countFeatureElement() {
        /* count the number of features based on '/resources/feature_mapper.csv'
            and number of features based on the '/resources/feature_mapper_no_value.csv'
         */
        int nbOfFeatures = featuresList.size() + featuresNoValueList.size();
        return nbOfFeatures;
    }

    public Double[] getFeatureWikidata(List<String> propertiesNoValue) {

        // get the features from feature mapper list files
        Double[] featureVector = new Double[featuresNoValueList.size()];
        Set<String> propertiesSet = Sets.newHashSet(propertiesNoValue);

        // put 1 if property for entities in Wikidata match with the list of 'resources/feature_mapper_no_value.csv', otherwise put 0
        int idx = 0;
        for (String propertyNoValue : featuresNoValueList) {
            // search the existance of a certain property in the list of property
            if (propertiesSet.contains(propertyNoValue)) {
                featureVector[idx] = 1.0;
            } else {
                featureVector[idx] = 0.0;
            }
            idx++;
        }

        return featureVector;
    }

    public Double[] getFeatureWikidata(Map<String, List<String>> properties) {

        Double[] featureVector = new Double[featuresList.size()];

        // collect the result of properties-values from the parameter
        Set<String> propertyValueKB = Sets.newHashSet();
        for (Map.Entry<String, List<String>> propertyGot : properties.entrySet()) {
            String property = propertyGot.getKey();
            List<String> values = propertyGot.getValue();
            // if values for the property exist
            if (values != null) {
                for (String value : values) {
                    String propertyValue = property + "_" + value;
                    propertyValueKB.add(propertyValue);
                }
            }
        }

        int idx = 0;
        // put 1 if property-value for entities in Wikidata match with the list of 'resources/feature_mapper.csv', otherwise put 0
        for (String propertyValue : featuresList) {
            // search the existance of a certain property-value in the list of property-value
            if (propertyValueKB.contains(propertyValue)) {
                featureVector[idx] = 1.0;
            } else {
                featureVector[idx] = 0.0;
            }
            idx++;
        }

        return featureVector;
    }

    // method to get wikidataId, label, real-predicted class, and properties in binary format (0-1)
    public WikidataElementInfos getFeatureWikidata(String wikidataId) {
        // count the number of features
        int nbOfFeatures = countFeatureElement();

        // get the element based on the wrapper whether from Wikidata or Nerd API
        WikidataElement wikidataElement = new WikidataElement();
        WikidataElementInfos wikidataElementInfos = new WikidataElementInfos();
        String label = null;
        Map<String, List<String>> propertiesWiki = new HashMap<>();
        List<String> featuresMap = new ArrayList<>();
        List<String> featuresNoValueList = new ArrayList<>();
        try {
            wikidataElement = wikidataFetcherWrapper.getElement(wikidataId); // wikidata Id, label, properties-values
        } catch (RuntimeException e) {
            LOGGER.info("Some errors encountered when collecting some features for a Wikidata Id \"" + wikidataId + "\"", e);
        } catch (Exception e) {
            LOGGER.info("Some errors encountered when getting some elements for a Wikidata Id \"" + wikidataId + "\"", e);
        }
        if (wikidataElement != null) {
            // set information of id, label, predicted class, features, real class
            wikidataElementInfos.setWikidataId(wikidataId);
            label = wikidataElement.getLabel();
            if (label != null)
                wikidataElementInfos.setLabel(wikidataElement.getLabel());

            // get the features from feature mapper list files
            featuresMap = featureFileExtractor.loadFeatures();
            featuresNoValueList = featureFileExtractor.loadFeaturesNoValue();
            // properties and values got directly from Wikidata or Nerd API (it depends on the implementation of the WikidataFetcherWrapper interface)
            propertiesWiki = wikidataElement.getProperties();

            // collect the result of properties-values fetched directly from Wikidata or Nerd KB
            List<String> propertyValueKB = new ArrayList<>();
            List<String> propertyNoValueKB = new ArrayList<>();
            if (propertiesWiki != null) {
                for (Map.Entry<String, List<String>> propertyGot : propertiesWiki.entrySet()) {
                    String property = propertyGot.getKey();
                    propertyNoValueKB.add(property);
                    List<String> values = propertyGot.getValue();
                    // if values for the property exist
                    if (values != null) {
                        for (String value : values) {
                            String propertyValue = property + "_" + value;
                            propertyValueKB.add(propertyValue);
                        }
                    }
                }
                // the index is based on the number of properties in both feature mapper file
                Double[] featureVector = new Double[nbOfFeatures];

                int idx = 0;

                // put 1 if property for entities in Wikidata match with the list of 'resources/feature_mapper_no_value.csv', otherwise put 0
                for (String propertyNoValue : featuresNoValueList) {
                    // search the existance of a certain property in the list of property
                    if (propertyNoValueKB.contains(propertyNoValue)) {
                        featureVector[idx] = 1.0;
                    } else {
                        featureVector[idx] = 0.0;
                    }
                    idx++;
                }

                // put 1 if property-value for entities in Wikidata match with the list of 'resources/feature_mapper.csv', otherwise put 0
                for (String propertyValue : featuresMap) {
                    // search the existance of a certain property-value in the list of property-value
                    if (propertyValueKB.contains(propertyValue)) {
                        featureVector[idx] = 1.0;
                    } else {
                        featureVector[idx] = 0.0;
                    }
                    idx++;
                }

                // set information of feature vector
                wikidataElementInfos.setFeatureVector(featureVector);
            }
        }
        return wikidataElementInfos;
    } // end of method 'getFeatureWikidata'

} // end of class FeatureWikidataExtractor
