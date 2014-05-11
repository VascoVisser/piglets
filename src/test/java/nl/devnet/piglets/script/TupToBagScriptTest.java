package nl.devnet.piglets.script;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.ExecType;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.Cluster;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.pigunit.pig.PigServer;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;


public class TupToBagScriptTest {

	@Test
	public void testRisPeers() throws IOException, ParseException {

		PigServer pigServer = new PigServer(ExecType.LOCAL);
		Cluster pigCluster = new Cluster(pigServer.getPigContext());
		PigTest test = new PigTest("src/test/pig/tup_to_bag.pig", null,
				pigServer, pigCluster);

		String[] input = { "1	(2,3)", };
		final String destination = "TupToBagTest.txt";
		PigTest.getCluster().copyFromLocalFile(input, destination, true);

		test.override("A",
				String.format("A = LOAD '%s' AS (f1:int, f2:());", destination));

		Iterator<Tuple> out = test.getAlias("out");

		assertEquals("Record 1", "(1,2)", out.next().toString());
		assertEquals("Record 2", "(1,3)", out.next().toString());
	}
}