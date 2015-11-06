package com.knockchat.utils;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedLongs;

public class UUID implements Comparable<UUID>, Serializable{
	
	  private static final class UUIDDomain extends DiscreteDomain<UUID> implements Serializable {
		    private static final UUIDDomain INSTANCE = new UUIDDomain();

		    @Override
		    public UUID next(UUID value) {
		      return value.equals(UUID.MAX_VALUE) ? null : value.inc();
		    }

		    @Override
		    public UUID previous(UUID value) {
		      return value.equals(UUID.MIN_VALUE) ? null : value.dec();
		    }

		    @Override
		    public long distance(UUID start, UUID end) {
		      return end.subtract(start).toLongNotOverflow();
		    }

		    @Override
		    public UUID minValue() {
		      return UUID.MIN_VALUE;
		    }

		    @Override
		    public UUID maxValue() {
		      return UUID.MAX_VALUE;
		    }

		    private Object readResolve() {
		      return INSTANCE;
		    }

		    @Override
		    public String toString() {
		      return "DiscreteDomain.uuids()";
		    }

		    private static final long serialVersionUID = 0;
		  }


	public static final DiscreteDomain<UUID> discreteDomain = UUIDDomain.INSTANCE; 

	
	public static final UUID MIN_VALUE = createWithString(0,0);
	public static final UUID MAX_VALUE = createWithString(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL);
	
	public static final UUID MIN_LONG = createWithString(0xFFFFFFFFFFFFFFFFL, Long.MIN_VALUE);
	public static final UUID MAX_LONG = createWithString(0, Long.MAX_VALUE);

    /**
     * Explicit serialVersionUID for interoperability.
     */
    private static final long serialVersionUID = -4856846361193249489L;

    /*
     * The most significant 64 bits of this UUID.
     *
     * @serial
     */
    private final long mostSigBits;

    /*
     * The least significant 64 bits of this UUID.
     *
     * @serial
     */
    private final long leastSigBits;
    
    private final String asString;

    private static final UUID[] cache = new UUID[1024*10];
    
    /*

    // Constructors and Factories

    /*
     * Private constructor which uses a byte array to construct the new UUID.
     */
    private UUID(byte[] data) {
        long msb = 0;
        long lsb = 0;
        assert data.length == 16 : "data must be 16 bytes in length";
        for (int i=0; i<8; i++)
            msb = (msb << 8) | (data[i] & 0xff);
        for (int i=8; i<16; i++)
            lsb = (lsb << 8) | (data[i] & 0xff);
        this.mostSigBits = msb;
        this.leastSigBits = lsb;
        this.asString = null;
    }

    /**
     * Constructs a new {@code UUID} using the specified data.  {@code
     * mostSigBits} is used for the most significant 64 bits of the {@code
     * UUID} and {@code leastSigBits} becomes the least significant 64 bits of
     * the {@code UUID}.
     *
     * @param  mostSigBits
     *         The most significant bits of the {@code UUID}
     *
     * @param  leastSigBits
     *         The least significant bits of the {@code UUID}
     */
    public UUID(long mostSigBits, long leastSigBits) {
        this.mostSigBits = mostSigBits;
        this.leastSigBits = leastSigBits;
        this.asString = null;
    }
    
    private UUID(long mostSigBits, long leastSigBits, String asString) {
        this.mostSigBits = mostSigBits;
        this.leastSigBits = leastSigBits;
        this.asString = asString;
    }
    
    private static UUID createWithString(long mostSigBits, long leastSigBits) {
    	return new UUID(mostSigBits, leastSigBits, new UUID(mostSigBits, leastSigBits).toString());
    }


    public static UUID fromLong(long leastSigBits){
    	return new UUID(0, leastSigBits);
    }
    
    public static UUID fromJdkUUID(java.util.UUID uuid){
    	if (uuid == null)
    		return null;
    	
    	return new UUID(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }
    
    public UUID(int space8bit, long most40Bits, long middle40Bits, long least40Bits){    	
		mostSigBits = (((long)(space8bit & 0xFF)) << 56) | (most40Bits << 16) | ((middle40Bits >> 24) & 0xFFFFL);
		leastSigBits = (middle40Bits << 40) | (least40Bits & 0xFFFFFFFFFFL);
		asString = null;
    }
    
	public int getSpace8bit(){
		return (int)((mostSigBits >> 56) & 0xFF);
	}

	public long getMost40Bits(){
		return (mostSigBits >> 16) & 0xFFFFFFFFFFL;
	}
	
	/**
	 * 
	 * @param uuid
	 * @return get middle 40bits
	 */
	public static long getAccountId(String uuid){
		return UUID.fromString(uuid).getMiddle40Bits();
	}

	/**
	 * 12bit - shardId
	 * 28bit - sequence id
	 * @return
	 */
	public long getMiddle40Bits(){
		return  ((mostSigBits & 0xFFFFL) << 24) | ((leastSigBits >> 40) & 0xFFFFFFL);
	}

	public long getLeast40Bits(){
		return leastSigBits & 0xFFFFFFFFFFL;
	}
	
	public static long pack(String uuid){
		return UUID.fromString(uuid).pack();
	}

	/**
	 * UNSAFE and temporary
	 * @return  packed UUID to 56 bit integer 
	 */
	public long pack(){
		
		final long space = getSpace8bit();
		final long middle = getMiddle40Bits();
		final long least = getLeast40Bits();
		final long most = getMost40Bits();
		
		if (space == 0){ //private chat ->  4/18/18/16
			
			if (most > 0x3FFFFL || middle > 0x3FFFFL || least > 0xFFFFL)
				throw new RuntimeException(String.format("UUID %s can not be packed: space=0x%x, most=0x%x, middle=0x%x, least=0x%x", toString(), space, most, middle, least));
		
			return ((space & 0x0FL) << 52) | ((most & 0x3FFFFL) << 34) | ((middle & 0x3FFFFL) << 16) | (least & 0xFFFFL);
		}else{
			
			if (most !=0)
				throw new RuntimeException(String.format("UUID %s can not be packed: space=0x%x, most=0x%x, middle=0x%x, least=0x%x", toString(), space, most, middle, least));

			if (middle > 0xFFFFFFFL || least > 0xFFFFFFL)
				throw new RuntimeException(String.format("UUID %s can not be packed: space=0x%x, most=0x%x, middle=0x%x, least=0x%x", toString(), space, most, middle, least));

			// 4/28/24
			return ((space & 0x0FL) << 52) | ((middle & 0xFFFFFFFL) << 24) | (getLeast40Bits() & 0xFFFFFFL);
		}				
	}
	
	public static String unpacks(long value){
		return unpack(value).toString();
	}
	
	/**
	 * UNSAFE and temporary
	 * @param value
	 * @return
	 */
	public static UUID unpack(long value){
		
		final int space = (int)((value >> 52) & 0x0FL);
		
		final long most, middle, least;

		if (space == 0){
			most = (value >> 34) & 0x3FFFFL;
			middle = (value >> 16) & 0x3FFFFL;
			least = value & 0xFFFFL;			
		}else{
			most = 0;
			middle = (value >> 24) & 0xFFFFFFFL;
			least = value & 0xFFFFFFL;			
		}
		
		return new UUID(space, most, middle, least);
	}
	
	public static List<String> unpack(List<Long> values){
		if (CollectionUtils.isEmpty(values))
			return (List)values;
		
		return Lists.transform(values, i -> (unpack(i).toString()));
	}

	public static List<Long> pack(List<String> values){
		if (CollectionUtils.isEmpty(values))
			return (List)values;
		
		return Lists.transform(values, i -> (pack(i)));
	}

    /**
     * Creates a {@code UUID} from the string standard representation as
     * described in the {@link #toString} method.
     *
     * @param  name
     *         A string that specifies a {@code UUID}
     *
     * @return  A {@code UUID} with the specified value
     *
     * @throws  IllegalArgumentException
     *          If name does not conform to the string representation as
     *          described in {@link #toString}
     *
     */
    public static UUID fromString(String name) {
    	
    	final int cacheKey = Math.abs(name.hashCode()) % cache.length;
    	final UUID cached = cache[cacheKey];
    	if (cached !=null && cached.asString.equals(name))
    		return cached;
    	
        String[] components = name.split("-");
        if (components.length != 5)
            throw new IllegalArgumentException("Invalid UUID string: "+name);
        for (int i=0; i<5; i++)
            components[i] = "0x"+components[i];

        long mostSigBits = Long.decode(components[0]).longValue();
        mostSigBits <<= 16;
        mostSigBits |= Long.decode(components[1]).longValue();
        mostSigBits <<= 16;
        mostSigBits |= Long.decode(components[2]).longValue();

        long leastSigBits = Long.decode(components[3]).longValue();
        leastSigBits <<= 48;
        leastSigBits |= Long.decode(components[4]).longValue();
        
        final UUID created =  new UUID(mostSigBits, leastSigBits, name);
        
        cache[cacheKey] = created;
        return created;
    }

    // Field Accessor Methods

    /**
     * Returns the least significant 64 bits of this UUID's 128 bit value.
     *
     * @return  The least significant 64 bits of this UUID's 128 bit value
     */
    public long getLeastSignificantBits() {
        return leastSigBits;
    }

    /**
     * Returns the most significant 64 bits of this UUID's 128 bit value.
     *
     * @return  The most significant 64 bits of this UUID's 128 bit value
     */
    public long getMostSignificantBits() {
        return mostSigBits;
    }


    // Object Inherited Methods

    /**
     * Returns a {@code String} object representing this {@code UUID}.
     *
     * <p> The UUID string representation is as described by this BNF:
     * <blockquote><pre>
     * {@code
     * UUID                   = <time_low> "-" <time_mid> "-"
     *                          <time_high_and_version> "-"
     *                          <variant_and_sequence> "-"
     *                          <node>
     * time_low               = 4*<hexOctet>
     * time_mid               = 2*<hexOctet>
     * time_high_and_version  = 2*<hexOctet>
     * variant_and_sequence   = 2*<hexOctet>
     * node                   = 6*<hexOctet>
     * hexOctet               = <hexDigit><hexDigit>
     * hexDigit               =
     *       "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9"
     *       | "a" | "b" | "c" | "d" | "e" | "f"
     *       | "A" | "B" | "C" | "D" | "E" | "F"
     * }</pre></blockquote>
     *
     * @return  A string representation of this {@code UUID}
     */
    @Override
	public String toString() {
    	if (this.asString !=null)
    		return asString;
    	else
	        return (digits(mostSigBits >> 32, 8) + "-" +
	                digits(mostSigBits >> 16, 4) + "-" +
	                digits(mostSigBits, 4) + "-" +
	                digits(leastSigBits >> 48, 4) + "-" +
	                digits(leastSigBits, 12));
    }
    
    public static List<String> toString(List<UUID> uuids){
    	return Lists.transform(uuids, UUID::toString);
    }

    /** Returns val represented by the specified number of hex digits. */
    private static String digits(long val, int digits) {
        long hi = 1L << (digits * 4);
        return Long.toHexString(hi | (val & (hi - 1))).substring(1);
    }

    /**
     * Returns a hash code for this {@code UUID}.
     *
     * @return  A hash code value for this {@code UUID}
     */
    @Override
	public int hashCode() {
        long hilo = mostSigBits ^ leastSigBits;
        return ((int)(hilo >> 32)) ^ (int) hilo;
    }

    /**
     * Compares this object to the specified object.  The result is {@code
     * true} if and only if the argument is not {@code null}, is a {@code UUID}
     * object, has the same variant, and contains the same value, bit for bit,
     * as this {@code UUID}.
     *
     * @param  obj
     *         The object to be compared
     *
     * @return  {@code true} if the objects are the same; {@code false}
     *          otherwise
     */
    @Override
	public boolean equals(Object obj) {
        if ((null == obj) || (obj.getClass() != UUID.class))
            return false;
        UUID id = (UUID)obj;
        return (mostSigBits == id.mostSigBits &&
                leastSigBits == id.leastSigBits);
    }

    // Comparison Operations

    /**
     * Compares this UUID with the specified UUID.
     *
     * <p> The first of two UUIDs is greater than the second if the most
     * significant field in which the UUIDs differ is greater for the first
     * UUID.
     *
     * @param  val
     *         {@code UUID} to which this {@code UUID} is to be compared
     *
     * @return  -1, 0 or 1 as this {@code UUID} is less than, equal to, or
     *          greater than {@code val}
     *
     */
    @Override
	public int compareTo(UUID val) {
    	final int r = UnsignedLongs.compare(this.mostSigBits, val.mostSigBits);
    	
    	return r==0 ? UnsignedLongs.compare(this.leastSigBits, val.leastSigBits) : r;
    }

    public int compareSigned(UUID val) {
    	final int r = Longs.compare(this.mostSigBits, val.mostSigBits);
    	
    	return r==0 ? UnsignedLongs.compare(this.leastSigBits, val.leastSigBits) : r;
    }

    public static UUID inc(UUID N)
    {
        long lo = (N.leastSigBits + 1);
        long hi = N.mostSigBits +  (((N.leastSigBits ^ lo) & N.leastSigBits) >>> 63);
        return new UUID(hi, lo);
    }
    
    public UUID inc(){
    	return inc(this);
    }
    
    public static UUID dec(UUID N)
    {
        long lo = (N.leastSigBits - 1);
        long hi = N.mostSigBits -  (((lo ^ N.leastSigBits) & lo) >>> 63);
        return new UUID(hi, lo);
    }

    public UUID dec(){
    	return dec(this);
    }

    public static UUID add(UUID N, UUID M)
    {
        long C = (((N.leastSigBits & M.leastSigBits) & 1L) + (N.leastSigBits >>> 1) + ((M.leastSigBits >>> 1)) >>> 63);
        long Hi = N.mostSigBits + M.mostSigBits + C;
        long Lo = N.leastSigBits + M.leastSigBits;
        return new UUID(Hi, Lo);
    }
    
    public UUID add(UUID M){
    	return add(this, M);
    }
    
    public static UUID subtract(UUID N, UUID M)
    {
        final long Lo = N.leastSigBits - M.leastSigBits;
        final long C = (((Lo & M.leastSigBits) & 1) + (M.leastSigBits >>> 1) + ((Lo >>> 1)) >>> 63);
        final long Hi = N.mostSigBits - (M.mostSigBits + C);
        return new UUID(Hi, Lo);
    }

    public UUID subtract(UUID M){
    	return subtract(this, M);
    }

    public static long toLongNotOverflow(UUID r){
    	    	
    	if (r.compareSigned(MIN_LONG)==-1)
    		return Long.MIN_VALUE;
    	else if (r.compareSigned(MAX_LONG)==1)
    		return Long.MAX_VALUE;
    	else 
    		return r.leastSigBits;
    }
    
    public long toLongNotOverflow(){
    	return toLongNotOverflow(this);
    }
    
    public static UUID fromBytes(byte []bytes){
    	ByteBuffer bb = ByteBuffer.wrap(bytes);
        long firstLong = bb.getLong();
        long secondLong = bb.getLong();
        return new UUID(firstLong, secondLong);    	
    }
 
    public byte[] asBytes(){    	
    	final ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(getMostSignificantBits());
        bb.putLong(getLeastSignificantBits());
        return bb.array();    	
    }

}
