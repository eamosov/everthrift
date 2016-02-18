package com.knockchat.utils;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.CharMatcher;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import gnu.trove.TLongCollection;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.procedure.TLongProcedure;

public class CollectionUtils {
	
	private static final int[] EMPTY_ARRAY= new int[0];
	private static final Integer[] EMPTY_INTEGR_ARRAY= new Integer[0];

	public static int[] toIntArray(List<? extends Number> list){
		if (list == null)
			return EMPTY_ARRAY;
		
		int ret[] = new int[list.size()];
		int i=0;
		for (Number s: list)
			ret[i++] = s.intValue();
		
		return ret;
	}

	public static Integer[] toIntegerArray(List<? extends Number> list){
		if (list == null)
			return EMPTY_INTEGR_ARRAY;
		
		Integer ret[] = new Integer[list.size()];
		int i=0;
		for (Number s: list)
			ret[i++] = new Integer(s.intValue());
		
		return ret;
	}
	
	public static List<Long> parseLongArray(String input){
		return 	Lists.newArrayList(Iterables.transform(Splitter.on(CharMatcher.anyOf(", ")).split(input), new Function<String, Long>(){

			@Override
			public Long apply(String input) {
				return Long.parseLong(input);
			}}));
	
	}
	
	public static <T> List<T> getInterseption(List<T> arr1, List<T> arr2){
	    final List<T> list=new ArrayList<T>();
	    
	    for (T i: arr1){
	          for (T j: arr2){
	              if(i.equals(j)){
	                    list.add(i);
	              }
	          }
	      }
	      return list;
	}	
	
	/**
	 * Returns a range of a list based on traditional offset/limit criteria.
	 *
	 * <p>Example:<pre>
	 *   ListUtil.subList(Arrays.asList(1, 2, 3, 4, 5), 3, 5) => [4,5]
	 * </pre></p>
	 *
	 * <p>In case the offset is higher than the list length the returned 
	 * sublist is empty (no exception).
	 * In case the list has fewer items than limit (with optional offset applied) 
	 * then the remaining items
	 * are returned (if any).</p>
	 *
	 * <p>Impl notes: returns a {@link List#subList} in all cases to have 
	 * a consistent return value.</p>
	 *
	 * @param list The input list.
	 * @param offset 0 for now offset, >=1 for an offset.
	 * @param limit -1 for no limit, >=0 for how many items to return at most, 
	 *              0 is allowed.
	 */
	public static <T> List<T> subList(List<T> list, int offset, int limit) {
	    if (offset<0) throw new IllegalArgumentException("Offset must be >=0 but was "+offset+"!");
	    if (limit<-1) throw new IllegalArgumentException("Limit must be >=-1 but was "+limit+"!");

	    if (offset>0) {
	        if (offset >= list.size()) {
	            return list.subList(0, 0); //return empty.
	        }
	        if (limit >-1) {
	            //apply offset and limit
	            return list.subList(offset, Math.min(offset+limit, list.size()));
	        } else {
	            //apply just offset
	            return list.subList(offset, list.size());
	        }
	    } else if (limit >-1) {
	        //apply just limit
	        return list.subList(0, Math.min(limit, list.size()));
	    } else {
	        return list.subList(0, list.size());
	    }
	}
	
	/**
	 * Получить размер пересечения двух сортированных массивов с уникальными элементами
	 * 
	 * @param a
	 * @param b
	 * @return
	 */
	public static int interseptionSize(List<? extends Comparable> a, List<? extends Comparable> b){
		
		int i=0;
		int j=0;
		
		Comparable last=null;
		int c=0;
		
		while((i<=a.size()-1) || (j<=b.size()-1)){
			
			Comparable cur;
			
			if (!(i<=a.size()-1)){
				cur = b.get(j++);
				//result.add(b.get(j++));
			}else if (!(j<=b.size()-1)){
				cur = a.get(i++);
			}else if (a.get(i).compareTo(b.get(j)) <= 0){
				cur = a.get(i++);
			}else{
				cur = b.get(j++);
			}
			
			if (last !=null && last.equals(cur))
				c++;
			
			last = cur;
		}
		
		return c;
	}
	
	public static List<String> lowerCase(List<String> input){
		final List<String> ret = Lists.newArrayListWithExpectedSize(input.size());
		
		for (String i: input)
			ret.add(StringUtils.lowerCase(i));
		
		return ret;
	}
	
	public static boolean contains(Collection<?> collection, Object element) {
		if (collection == null || collection.isEmpty())
			return false;
		else
			return collection.contains(element);
	}
	
	public static boolean isEmpty(TLongCollection c){
		return c==null || c.isEmpty();
	}
	
	public static <T> boolean isEmpty(Collection<T> c){
		return c==null || c.isEmpty();
	}

	public static List<Long> asList(TLongCollection c){
		if (c==null || c.isEmpty())
			return Collections.emptyList();
		
    	final List<Long> _ids = Lists.newArrayListWithCapacity(c.size());
    	c.forEach(new TLongProcedure(){

			@Override
			public boolean execute(long value) {
				_ids.add(value);
				return true;
			}});
    	
    	return _ids;
	}
	
	public static <T> TLongObjectMap<T> asTLongObjectMap(Map<Long, T> m){
		final TLongObjectMap<T> r;
		
		if (m == null)
			return null;
		
		if (!m.isEmpty()){
			r = new TLongObjectHashMap<T>(m.size());
		
			for (Entry<Long, T> e: m.entrySet())
				r.put(e.getKey(), e.getValue());
		}else{
			r = new TLongObjectHashMap<T>();
		}
		
		return r; 
	}
	
	public static <T> void optimisticFilter(List<T> input, Predicate<? super T> p){
		
		if (input == null || input.isEmpty())
			return;
		
		for (int i=0; i< input.size(); i++){
			if (p.apply(input.get(i)) == false){
				final ListIterator<T> it = input.listIterator(i);
				while(it.hasNext()){
					if (!p.apply(it.next()))
						it.remove();
				}
				return;
			}
		}
	}
	
	static class JoinedCollectionView<E> implements Collection<E> {

		private final Collection<? extends E>[] items;

		public JoinedCollectionView(final Collection<? extends E>[] items) {
			this.items = items;
		}

		@Override
		public boolean addAll(final Collection<? extends E> c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void clear() {
			for (final Collection<? extends E> coll : items) {
				coll.clear();
			}
		}

		@Override
		public boolean contains(final Object o) {
			for (final Collection<? extends E> coll : items) {
				if (coll.contains(o))
					return true;
			}
			return false;
		}

		@Override
		public boolean containsAll(final Collection<?> c) {
			for (Object o:c){
				if (!contains(o))
					return false;
			}
			
			return true;			
		}

		@Override
		public boolean isEmpty() {
			for (final Collection<? extends E> coll : items) {
				if (!coll.isEmpty())
					return false;
			}
			return true;
		}

		@Override
		public Iterator<E> iterator() {
			return Iterables.concat(items).iterator();
		}

		@Override
		public boolean remove(final Object o) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean removeAll(final Collection<?> c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean retainAll(final Collection<?> c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public int size() {
			int ct = 0;
			for (final Collection<? extends E> coll : items) {
				ct += coll.size();
			}
			return ct;
		}

		@Override
		public Object[] toArray() {
			Object arr[] = new Object[size()];
			int i=0;
			Iterator<E> it = this.iterator();
			while(it.hasNext()){
				arr[i++] = it.next();
			}
			return arr;
		}

		@Override
		public <T> T[] toArray(T[] a) {
			final int _size = size();
			final Object arr[];
			if (a.length < _size){
				arr = (Object[])Array.newInstance(a.getClass().getComponentType(), size());
			}else{
				arr = a;
			}
			
			int i=0;
			Iterator<E> it = this.iterator();
			while(it.hasNext()){
				arr[i++] = it.next();
			}
			return (T[])arr;
		}

		@Override
		public boolean add(E e) {
			throw new UnsupportedOperationException();
		}

	}

	/**
	 * Returns a live aggregated collection view of the collections passed in.
	 * <p>
	 * All methods except {@link Collection#size()}, {@link Collection#clear()},
	 * {@link Collection#isEmpty()} and {@link Iterable#iterator()} throw
	 * {@link UnsupportedOperationException} in the returned Collection.
	 * <p>
	 * None of the above methods is thread safe (nor would there be an easy way
	 * of making them).
	 */
	public static <T> Collection<T> combine(final Collection<? extends T>... items) {
		return new JoinedCollectionView<T>(items);
	}
	
	public static <T> HashSet<T> newHashSet(final Collection<? extends T> ...collections){
		final HashSet<T> ret = Sets.newHashSet();		
		
		if (collections !=null){
			for (Collection<? extends T> c: collections){
				if (c!=null)
					ret.addAll(c);
			}			
		}
		
		return ret;
	}
	
	public static int cardinality(boolean ...values){
		int c=0;
		for (int j=0; j<values.length; j++)
			c += values[j] ? 1: 0;
		return c;
	}
	
	public static <T> void aggregate(Iterator<T> it, int size, VoidFunction<List<T>> f) {		
		Iterators.partition(it, size).forEachRemaining(a -> {f.apply(a);});
	}

	public static boolean equalsIgnoreCase(Collection<String> where, String what){
		if (where == null || where.isEmpty())
			return false;
		
		for (String w: where){
			if (w.equalsIgnoreCase(what))
				return true;
		}
		
		return false;
	}
}
