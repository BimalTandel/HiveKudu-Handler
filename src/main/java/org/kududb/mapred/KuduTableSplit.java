package org.kududb.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.stream.Collectors;

import org.apache.kudu.client.KuduScanToken;

public class KuduTableSplit implements org.apache.hadoop.mapred.InputSplit {

	private byte[] scanTokenSerialized;
	private String[] locations;

	public static KuduTableSplit of(KuduScanToken scanToken) throws IOException {
		KuduTableSplit tableSplit = new KuduTableSplit();
		tableSplit.scanTokenSerialized = scanToken.serialize();
		tableSplit.locations = scanToken.getTablet().getReplicas().stream().map(replica -> replica.getRpcHost())
				.collect(Collectors.toList()).toArray(new String[0]);
		return tableSplit;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// Write scanTokenSerialized
		out.writeInt(this.scanTokenSerialized.length);
		out.write(this.scanTokenSerialized);
		// Write locations
		out.writeInt(this.locations.length);
		for (String loc : this.locations)
			out.writeUTF(loc);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// read scanTokenSerialized
		int byteArrayLength = in.readInt();
		this.scanTokenSerialized = new byte[byteArrayLength];
		in.readFully(scanTokenSerialized);
		// read locations
		int locationsLength = in.readInt();
		this.locations = new String[locationsLength];
		for (int i = 0; i < locationsLength; ++i)
			this.locations[i] = in.readUTF();
	}

	@Override
	public long getLength() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String[] getLocations() throws IOException {
		return this.locations.clone();
	}

	byte[] getScanTokenSerialized() {
		return this.scanTokenSerialized.clone();
	}
}
