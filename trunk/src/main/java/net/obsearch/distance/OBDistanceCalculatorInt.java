package net.obsearch.distance;

import java.util.ArrayList;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import net.obsearch.exception.OBException;
import net.obsearch.ob.OBInt;
import net.obsearch.query.OBQueryInt;

public final class OBDistanceCalculatorInt<O extends OBInt> {

	private static final transient Logger logger = Logger
			.getLogger(OBDistanceCalculatorInt.class);

	private boolean[] available;
	private Exception e = null;
	private Exec<O>[] execs;
	private Thread[] th;
	private Semaphore sem;
	private final int threadCount;

	public OBDistanceCalculatorInt(int threads) {
		available = new boolean[threads];

		execs = new Exec[threads];
		th = new Thread[threads];
		int i = 0;
		while (i < available.length) {
			available[i] = true;
			execs[i] = new Exec<O>(i);
			th[i] = new Thread(execs[i]);
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
	public void process(long idObj, O obj, O q, OBQueryInt<O> query)
			throws OBException {
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
		e.init(idObj, obj, q, query);
		boolean interrupted = false;
		do {
			try {
				th[i].join();
			} catch (InterruptedException m) {
				interrupted = true;
			}
		} while (interrupted);
		th[i].start();

	}

	private final class Exec<OB extends OBInt> implements Runnable {
		private OB obj;
		private OB q;
		private OBQueryInt<OB> query;
		private long idObj;
		private int threadId;

		public Exec(int threadId) {
			this.threadId = threadId;
		}

		public void init(long idObj, OB obj, OB q, OBQueryInt<OB> query) {
			this.idObj = idObj;
			this.obj = obj;
			this.q = q;
			this.query = query;
		}

		@Override
		public void run() {
			try {
				int realDistance = obj.distance(q);
				if (realDistance <= query.getDistance()) {
					query.add(idObj, obj, realDistance);
				}
				available[threadId] = true;
				sem.release();
			} catch (Exception ex) {
				logger.fatal(ex);
				synchronized (available) {
					e = ex;
				}

			}
		}
	}

}
