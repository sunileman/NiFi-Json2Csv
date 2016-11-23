package org.nifi.custom;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.nifi.custom.JsonToCsv.HEADER_AND_BODY;
import static org.nifi.custom.JsonToCsv.JSON_PATH;

/**
 *
 * @author sunile manjee
 */
public class JsonToCsvProcessorTest {

    /**
     * Test of onTrigger method, of class JsonToCsvProcessor.
     */
    @org.junit.Test
    public void testOnTrigger() throws IOException {


        String json = "{\n" +
                "    \"id\": 1,\n" +
                "    \"name\": \"A green door\",\n" +
                "    \"price\": 12.50,\n" +
                "    \"tags\": [\"home\", \"green\"]\n" +
                "}";

        InputStream content = new ByteArrayInputStream(json.getBytes());

        // Generate a test runner to mock a processor in a flow
        TestRunner runner = TestRunners.newTestRunner(new JsonToCsv());

        // Add properties
        runner.setProperty(JsonToCsv.OUTPUTHANDLER, HEADER_AND_BODY.getValue());

        // Add the content to the runner
        runner.enqueue(content);

        // Run the enqueued content, it also takes an int = number of contents queued
        runner.run(1);

        // All results were processed with out failure
        runner.assertQueueEmpty();

        // If you need to read or do additional tests on results you can access the content
        List<MockFlowFile> results = runner.getFlowFilesForRelationship(JsonToCsv.SUCCESS);
        //assertTrue("1 match", results.size() == 1);
        MockFlowFile result = results.get(0);
        String resultValue = new String(runner.getContentAsByteArray(result));
        System.out.println("Match: " + IOUtils.toString(runner.getContentAsByteArray(result)));

        // Test attributes and content
        result.assertAttributeEquals(JsonToCsv.MATCH_ATTR, "id,name,price,tags1,tags2\n" +
                "1,A green door,12.5,home,green");
        result.assertContentEquals("id,name,price,tags1,tags2\n" +
                "1,A green door,12.5,home,green");

    }

}