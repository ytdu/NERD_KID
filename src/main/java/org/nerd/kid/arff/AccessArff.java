package org.nerd.kid.arff;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class AccessArff {
    public String[] readClassArff(File file) throws Exception{
        // variables
        String line = "";
        String startClassAttribute = "@ATTRIBUTE class {";
        String endClassAttribute ="}";
        String[] classData = null;
        String splitBy = ",";

        //read a file
        BufferedReader reader = new BufferedReader(new FileReader(file));

        // getting the data of class
        while((line = reader.readLine()) != null){
            if (line.startsWith("@ATTRIBUTE")){
                if(line.contains("class")){
                    // removing start and end of symbols of class
                    String result = line.replace(startClassAttribute,"").replace(endClassAttribute,"");
                    classData = result.split(splitBy);
                }
            }

        }
        reader.close();

        return classData;
    }

    public void readAttributeArff() throws Exception{

    }

    public void createArff() throws Exception{

    }
}