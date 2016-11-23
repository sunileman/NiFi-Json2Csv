package com.jsontocsv.writer;

import org.apache.commons.lang.StringUtils;
import org.apache.nifi.processor.ProcessContext;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;


import static org.nifi.custom.JsonToCsv.*;

public class CSVWriter {

    public String writeAsCSV(List<Map<String, String>> flatJson, ProcessContext context) throws FileNotFoundException {
        Set<String> headers = collectHeaders(flatJson);
        String output = new String();

        if (context.getProperty(OUTPUTHANDLER).getValue().equals(HEADER_AND_BODY.getValue())){
             output = StringUtils.join(headers.toArray(), ",") + "\n";
            for (Map<String, String> map : flatJson) {
                output = output + getCommaSeperatedRow(headers, map) + "\n";
            }
        } else if (context.getProperty(OUTPUTHANDLER).getValue().equals(HEADER_ONLY.getValue())){
             output = StringUtils.join(headers.toArray(), ",") + "\n";
        } else if (context.getProperty(OUTPUTHANDLER).getValue().equals(BODY_ONLY.getValue())){
            for (Map<String, String> map : flatJson) {
                output = output + getCommaSeperatedRow(headers, map) + "\n";
            }
        }

        System.out.println(output.substring(0, output.length()-1));

        return output.substring(0, output.length()-1);

    }

    /*
    public void writeAsCSV(List<Map<String, String>> flatJson, String fileName) throws FileNotFoundException {
        Set<String> headers = collectHeaders(flatJson);
        String output = StringUtils.join(headers.toArray(), ",") + "\n";
        for (Map<String, String> map : flatJson) {
            output = output + getCommaSeperatedRow(headers, map) + "\n";
        }
        System.out.println(output);
        writeToFile(output, fileName);
    }
    */

    private void writeToFile(String output, String fileName) throws FileNotFoundException {
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(fileName));
            writer.write(output);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close(writer);
        }
    }

    private void close(BufferedWriter writer) {
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String getCommaSeperatedRow(Set<String> headers, Map<String, String> map) {
        List<String> items = new ArrayList<String>();
        for (String header : headers) {
            String value = map.get(header) == null ? "" : map.get(header).replace(",", "");
            items.add(value);
        }
        return StringUtils.join(items.toArray(), ",");
    }

    private Set<String> collectHeaders(List<Map<String, String>> flatJson) {
        Set<String> headers = new TreeSet<String>();
        for (Map<String, String> map : flatJson) {
            headers.addAll(map.keySet());
        }
        return headers;
    }
}