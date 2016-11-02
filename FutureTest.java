
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.RecursiveTask;

/**
 * Simple demo of Executors and Fork/Join
 * 
 * Based on https://blog.codecentric.de/en/2011/10/executing-tasks-in-parallel-using-java-future/
 */
public class FutureTest {

	private static final int NUM_THREADS = 3;
	private static final int MAX_NUMBER = 2_000_000_000;
	private static final int THRESHOLD = 100_000;

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		System.out.println("Basic implementation:");
		long start = System.currentTimeMillis();
		System.out.println("count = " + divisibleBy(0, MAX_NUMBER, 4));
		long end = System.currentTimeMillis();
		System.out.println("time taken = " + (end - start));

		System.out.println("\nExecutorService implementation:");
		start = System.currentTimeMillis();
		System.out.println("count = " + divisibleByFuture(0, MAX_NUMBER, 4));
		end = System.currentTimeMillis();
		System.out.println("time taken= " + (end - start));
		
		System.out.println("\nFork/Join implementation:");
		start = System.currentTimeMillis();
		System.out.println("count = " + divisibleByFutureFork(0, MAX_NUMBER, 4));
		end = System.currentTimeMillis();
		System.out.println("time taken = " + (end - start));
	}
	
	/**
	 * Basic implementation
	 */
	public static int divisibleBy(int first, int last, int divisor) {
		int count = 0;
		for (int i = first; i <= last; i++) {
			if (i % divisor == 0) {
				count++;
			}
		}
		return count;
	}

	/**
	 * Implementation using ExecutorService to split this up into N tasks
	 */
	public static int divisibleByFuture(int first, int last, int divisor) throws InterruptedException, ExecutionException {		
		int count = 0;

		ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
		List<Future<Integer>> taskList = new ArrayList<>();
		
		for (int i = first; i < MAX_NUMBER; i += THRESHOLD + 1) {
			final int fst = i;
			final int lst = Math.min(last, i + THRESHOLD);
			Future<Integer> futureTask = executor.submit(() -> {
				return FutureTest.divisibleBy(fst, lst, divisor);
			});
			taskList.add(futureTask);	
		}

		for (Future<Integer> t : taskList) {
			count += t.get();
		}
		executor.shutdown();

		return count;
	}	
	
	/**
	 * Implementation using fork and join
	 */
	public static int divisibleByFutureFork(int first, int last, int divisor) throws InterruptedException, ExecutionException {
		ForkJoinPool mainPool = new ForkJoinPool(NUM_THREADS);
		DivisbleBy divisibleBy = new DivisbleBy(first, last, divisor);
		Integer result = mainPool.invoke(divisibleBy);
		System.out.println("Steal count: " + mainPool.getStealCount());
		
		return result;
	}	

	/**
	 * ForkJoinTask implementation
	 */
	static class DivisbleBy extends RecursiveTask<Integer> {

		
		private int first;
		private int last;
		private int divisor;
		
		DivisbleBy(int first, int last, int divisor) {
			this.first = first;
			this.last = last;
			this.divisor = divisor;
		}
		/**
		 * Recursive function. If last - first < THRESHOLD then do computation. Otherwise split
		 * and call the function recursively
		 */
		@Override
		protected Integer compute() {
			int amount = last - first;
			if (amount <= THRESHOLD) {
				return computeDirectly();
			} else {
				int count = 0;
				
				amount = amount / 2;
				DivisbleBy left = new DivisbleBy(first, first + amount, divisor);
				DivisbleBy right = new DivisbleBy(first + amount + 1, last, divisor);
				left.fork();
				count = right.compute() + left.join();
				
				return count;
			}
		}
		
		private Integer computeDirectly() {
			int count = 0;
			for (int i = first; i <= last; i++) {
				if (i % divisor == 0) {
					count++;
				}
			}
			return count;
		}
		
	}
}
