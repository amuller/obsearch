package net.obsearch.distance;

import java.util.ArrayList;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import net.obsearch.exception.OBException;
import net.obsearch.filter.Filter;
import net.obsearch.ob.OBInt;
import net.obsearch.query.OBQueryInt;

public final class OBDistanceCalculatorInt<O extends OBInt> {

	private static final transient Logger logger = Logger
			.getLogger(OBDistanceCalculatorInt.class);

	private boolean[] available;
	private Exception e = null;
	private Exec<O>[] execs;
	private Semaphore sem;
	private final int threadCount;

	public OBDistanceCalculatorInt(int threads) {
		available = new boolean[threads];

		execs = new Exec[threads];

		int i = 0;
		while (i < available.length) {
			available[i] = true;
			execs[i] = new Exec<O>(i);
			new Thread(execs[i], "Dist-" + i).start();
			i++;
		}
		this.threadCount = threads;
		sem = new Semaphore(threads);
	}

	/**
	 * Process asyncrhonously a and b.
	 * 
	 * @param a
	 * @param b
	 * @param query
	 * @throws Exception
	 */
	public void process(long idObj, O obj, O q, OBQueryInt<O> query,
			Filter<O> filter) throws OBException {
		if (e != null) {
			throw new OBException(e);
		}
		sem.acquireUninterruptibly(); // only work if there are free threads.
		// free permit implies that at least one thread is waiting
		// to receive orders.
		int i = 0;
		while (i < threadCount) {
			if (available[i]) {
				break;
			}
			i++;
		}
		// thread i is ready to be used.
		Exec<O> e = execs[i];
		e.init(idObj, obj, q, query, filter);
		e.go();

	}

	private final class Exec<OB extends OBInt> implements Runnable {
		private OB obj;
		private OB q;
		private OBQueryInt<OB> queryResult;
		private long idObj;
		private int threadId;
		private Semaphore control;
		private Filter<OB> filter;

		public Exec(int threadId) {
			this.threadId = threadId;
			control = new Semaphore(0);
		}

		private void go() {
			control.release();
		}

		public void init(long idObj, OB obj, OB q, OBQueryInt<OB> query,
				Filter<OB> filter) {
			this.idObj = idObj;
			this.obj = obj;
			this.q = q;
			this.queryResult = query;
			this.filter = filter;
		}

		@Override
		public void run() {
			try {
				while (true) {
					control.acquireUninterruptibly();
					if (filter == null || filter.accept(obj, q)) {
						int realDistance = obj.distance(q);
						if (realDistance <= queryResult.getDistance()) {
							queryResult.add(idObj, obj, realDistance);
						}
					}
					available[threadId] = true;
					sem.release();
				}
			} catch (Exception ex) {
				logger.fatal(ex);
				synchronized (available) {
					e = ex;
				}

			}
		}
	}

}
