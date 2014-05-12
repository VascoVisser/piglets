package nl.devnet.piglets.script;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.pig.ExecType;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.Cluster;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.pigunit.pig.PigServer;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;


public class TupToBagScriptTest {

	@Test
	public void testRisPeers() throws IOException, ParseException {

		PigServer pigServer = new PigServer(ExecType.LOCAL);
		Cluster pigCluster = new Cluster(pigServer.getPigContext());
		PigTest test = new PigTest("src/test/pig/tup_to_bag.pig", null,
				pigServer, pigCluster);

		String[] input = { "1	(2,3)", 
				"2	2",
				"3	(,,)"};
		final String destination = "TupToBagTest.txt";
		PigTest.getCluster().copyFromLocalFile(input, destination, true);

		test.override("A",
				String.format("A = LOAD '%s' AS (f1:int, f2:());", destination));

		List<Tuple> out = Lists.newArrayList(test.getAlias("out"));

		System.out.println(Joiner.on("\n").join(out));
		
		int i = 0;
		assertEquals("Record 1", "(1,2)", out.get(i++).toString());
		assertEquals("Record 2", "(1,3)", out.get(i++).toString());
		assertEquals("Record 3", "(2,)", out.get(i++).toString());
		assertEquals("Record 4", "(3,)", out.get(i++).toString());
		assertEquals("Record 5", "(3,)", out.get(i++).toString());
		assertEquals("Record 6", "(3,)", out.get(i++).toString());
	}
}