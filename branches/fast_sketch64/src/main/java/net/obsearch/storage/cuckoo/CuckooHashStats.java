package net.obsearch.storage.cuckoo;

import hep.aida.bin.StaticBin1D;

public class CuckooHashStats {

	/**
	 * Size of the buckets for gets. (how many objects are stuck into one
	 * address)
	 */
	private StaticBin1D getLength = new StaticBin1D();

	/**
	 * Frag report
	 */
	private StaticBin1D fragReport;

	public StaticBin1D getGetLength() {
		return getLength;
	}

	/**
	 * Size of the buckets we had
	 */
	private StaticBin1D bucketDepth = new StaticBin1D();
	/**
	 * # of inserts at h1 level
	 */
	private long h1Inserts = 0;
	/**
	 * # of inserts at h2 level
	 */
	private long h2Inserts = 0;

	private long memReleases = 0;
	private long memRequests = 0;
	private long memRecycle = 0;

	public void incMemReleases() {
		memReleases++;
	}

	public void incMemRequests() {
		memRequests++;
	}

	public void incMemRecycles() {
		memRecycle++;
	}

	public void incH1Inserts() {
		h1Inserts++;
	}

	public void incH2Inserts() {
		h2Inserts++;
	}

	public StaticBin1D getBucketDepth() {
		return bucketDepth;
	}

	public long getH1Inserts() {
		return h1Inserts;
	}

	public long getH2Inserts() {

		return h2Inserts;
	}

	public void addGetLength(double arg0) {
		getLength.add(arg0);
	}

	public void addDepth(double arg0) {
		bucketDepth.add(arg0);
	}

	public String toString() {
		return "H1: " + h1Inserts + " H2: " + h2Inserts + "\n Depth:\n"
				+ stat(bucketDepth) + "\n get length:\n" + stat(getLength)
				+ "\n frag: \n" + stat(this.fragReport) + "\n"
				+ "req: " + this.memRequests + " rel: " + this.memReleases + " recy: " + this.memRecycle;
				
	}

	private String stat(StaticBin1D s) {
		return "mean: " + s.mean() + " std: " + s.standardDeviation()
				+ " max: " + s.max() + " min: " + s.min() + " count: "
				+ s.size();
	}

	public StaticBin1D getFragReport() {
		return fragReport;
	}

	public void setFragReport(StaticBin1D fragReport) {
		this.fragReport = fragReport;
	}

}
