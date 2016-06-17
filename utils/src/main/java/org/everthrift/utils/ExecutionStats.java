package org.everthrift.utils;

import java.util.List;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class ExecutionStats {

	/**
	 * Колчиество выполнений
	 */
	private long count;

	/**
	 * Суммарное время выполнения
	 */
	private long summaryTime;

	/**
	 * сумма квадратов времен выполнения
	 */
	private long sqSummaryTime;
	
	/**
	 * колличество попадания велечины в интервал Avr-S Avr+S
	 */
	private long pS;
	
	/**
	 * колличество попадания велечины в интервал Avr-S Avr+S
	 */
	private long p2S;
	

	public ExecutionStats() {
		count = 0;
		summaryTime = 0;
		sqSummaryTime = 0;
		pS = 0;
		p2S = 0;
	}

	public ExecutionStats(ExecutionStats old) {
		count = old.count;
		summaryTime = old.summaryTime;
		sqSummaryTime = old.sqSummaryTime;
		pS = old.pS;
		p2S = old.p2S;
	}
	
	public ExecutionStats( ExecutionStats stats, long time ) {
		this(stats);
		this.update(time);
	}
	
	synchronized public void update(long time){
		count = count + 1;
		summaryTime = summaryTime + time;
		sqSummaryTime = sqSummaryTime + time*time;
		
		if (Math.abs(time - getAverageTime()) <= (long)getS())
			pS ++;
		else if (Math.abs(time - getAverageTime()) <= (long)getS() * 2)
			p2S ++;

	}

	/**
	 * Получить количество выполнений
	 * @return количество выполнений
	 */
	public long getCount() {
		return count;
	}

	/**
	 * Получить суммарное время выполнения
	 * @return суммарное время выполнения
	 */
	public long getSummaryTime() {
		return summaryTime;
	}

	/**
	 * Получить среднее время выполнения
	 * @return среднее время выполнения
	 */
	public long getAverageTime() {
		if (count == 0)
			return 0;
		
		return summaryTime / count;
	}
	
	public double getDispertion(){
		if (count == 0)
			return 0;
		
		return (double)sqSummaryTime / (double)count - Math.pow((double)summaryTime / (double)count, 2) ;
	}
	
	public double getS(){
		if (count < 2)
			return 0;
		
		return Math.sqrt(getDispertion() * (double)count / ((double)count -1.0));
	}
	
	public long getPs1(){
		if (count == 0)
			return 0;
		return pS * 100 / count;
	}

	public long getPs2(){
		if (count == 0)
			return 0;
		
		return p2S * 100 / count;
	}
	
	public static String getLogString(List<Pair<String,ExecutionStats>> list){
		long sumTime=0;
		long sumCount = 0;
		
		for ( Pair<String,ExecutionStats> p : list ){
			sumTime += p.second.getSummaryTime();
			sumCount += p.second.getCount();
		}
		
		final StringBuilder wr = new StringBuilder();
		
		wr.append(String.format("%10s, %2s, %8s, %2s, %6s, %9s, %3s, %3s, \"%s\"\n", "sum mcs", "%%", "count", "%%", "avr mcs", "S", "pS1", "pS2", "title"));
		
		for ( Pair<String,ExecutionStats> p : list )
			wr.append(String.format("%10d, %2d, %8d, %2d, %7d, %9.2f, %3d, %3d, \"%s\"\n",
					 p.second.getSummaryTime(), //всего времени в mcs
					 p.second.getSummaryTime() * 100 / sumTime, //время в процентах от общего времени, затраченного на выполнение всех запросов
					 p.second.getCount(), //кол-во запросов
					 p.second.getCount() * 100 / sumCount, //процент от общего кол-ва запросов
					 p.second.getAverageTime(), //среднее время выполенния запроса
					 p.second.getS(), //стандартное отклонение
					 p.second.getPs1(), //процент попадания в интервал [-S;+S]
					 p.second.getPs2(), //процент попадания в интервал -[2S;+2S]
					 p.first));
		
		return wr.toString();
	}
}
