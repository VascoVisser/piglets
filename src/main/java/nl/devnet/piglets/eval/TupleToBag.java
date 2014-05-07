package nl.devnet.piglets.eval;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.SchemaUtil;

import com.google.common.collect.Lists;

/**
 * This UDF explodes a tuple into a big. It does what one would expected from
 * doing TOBAG(FLATTEN(x)) where x is a single tuple. However, FLATTEN is not a
 * function but a language construct and it is not allowed inside a function
 * call.
 * 
 * @author Vasco Visser
 */
public class TupleToBag extends EvalFunc<DataBag> {

	public DataBag exec(Tuple input) throws IOException {
		if (null == input || input.size() == 0)
			return null;

		if (!(input.get(0) instanceof Tuple))
			throw new IOException("Input not a tuple");

		Tuple in = (Tuple) input.get(0);
		DataBag db = BagFactory.getInstance().newDefaultBag();

		for (Object o : in)
			db.add(TupleFactory.getInstance().newTuple(o));

		return db;
	}

	public Schema outputSchema(Schema input) {
		try {
			return SchemaUtil.newBagSchema(Lists
					.newArrayList(DataType.BYTEARRAY));
		} catch (Exception e) {
			return null;
		}
	}
}