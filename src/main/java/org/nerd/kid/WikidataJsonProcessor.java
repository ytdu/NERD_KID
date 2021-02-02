package org.nerd.kid;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Sets;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.nerd.kid.data.WikidataElement;
import org.nerd.kid.data.WikidataElementInfos;
import org.nerd.kid.extractor.wikidata.WikibaseWrapper;
import org.nerd.kid.extractor.wikidata.WikidataFetcherWrapper;
import org.nerd.kid.model.WikidataNERPredictor;

import java.io.*;
import java.util.*;

public class WikidataJsonProcessor {
    public static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static Set<String> KV_FEATURE_PROPERTIES = Sets.newHashSet(
            "P21", "P279", "P31", "P361"
    );


    static {
        OBJECT_MAPPER.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        OBJECT_MAPPER.setVisibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.NONE);
        OBJECT_MAPPER.setVisibility(PropertyAccessor.SETTER, JsonAutoDetect.Visibility.NONE);
        OBJECT_MAPPER.setVisibility(PropertyAccessor.IS_GETTER, JsonAutoDetect.Visibility.NONE);
        OBJECT_MAPPER.configure(SerializationFeature.WRITE_DATE_KEYS_AS_TIMESTAMPS, true);
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static WikidataElement fromWikidataJson(String inputInJson) throws IOException {
        JsonNode root = OBJECT_MAPPER.readTree(inputInJson);
        WikidataElement element = new WikidataElement();
        String wikidataId = root.get("id").asText();
        element.setId(wikidataId);
        try {
            JsonNode labels = root.get("labels");
            if (labels.has("en")) {
                JsonNode en = labels.get("en");
                String label = en.get("value").asText();
                label = label.replace(",", ";");
                label = label.replace("\"", "");
                label = label.replace("\'", "");
                element.setLabel(label);
            }
        } catch (Exception e) {
            element.setLabel("");
        }
        JsonNode claims = root.get("claims");
        Map<String, List<String>> properties = element.getProperties();
        List<String> noValueProperties = element.getPropertiesNoValue();
        for (Iterator<String> it = claims.fieldNames(); it.hasNext(); ) {
            String property = it.next();
            if (!KV_FEATURE_PROPERTIES.contains(property)) {
                noValueProperties.add(property);
                continue;
            }
            JsonNode snaks = claims.get(property);
            List<String> values = new ArrayList<>();
            properties.put(property, values);

            for (JsonNode snak : snaks) {
                try {
                    JsonNode mainsnak = snak.get("mainsnak");
                    String value = null;
                    if ("wikibase-item".equals(mainsnak.get("datatype").asText())) {
                        value = mainsnak.get("datavalue").get("value").get("id").asText();
                    }
                    properties.get(property).add(value);
                } catch (Exception e) {
                    System.out.println("Hit Exception when parsing " + wikidataId + ":" + property);
                    continue;
                }
            }
        }

        return element;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("usage: <input wikidata file (.json[.bz2])> <output file (.tsv)>");
        }
        // statements collected from entity-fishing API Service (http://nerd.huma-num.fr/nerd/service/kb/concept)
        WikidataFetcherWrapper wrapper = new WikibaseWrapper();
        WikidataNERPredictor predictor = new WikidataNERPredictor(wrapper);
        InputStream inputStream;
        if (args[0].endsWith(".bz2")) {
            inputStream = new BZip2CompressorInputStream(new FileInputStream(args[0]));
        } else {
            inputStream = new FileInputStream(args[0]);
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        BufferedWriter bw = new BufferedWriter(new FileWriter(args[1]));
        String line;
        int ln = 0;
        int printInterval = 1;
        while ((line = br.readLine()) != null) {
            ln++;
            if (ln % printInterval == 0) {
                System.out.println(ln);
            }
            if (ln >= printInterval * 10) {
                printInterval *= 10;
            }
            if ("[".equals(line) || "]".equals(line)) {
                continue;
            }
            if (line.endsWith(",")) {
                line = line.substring(0, line.length() - 1);
            }
            WikidataElement element = fromWikidataJson(line);
            WikidataElementInfos infos = predictor.predict(element);
            bw.write(element.getId() + "\t" + infos.getPredictedClass() + "\n");
        }
        br.close();
        bw.close();
    }
}
