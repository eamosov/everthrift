package com.knockchat.sql.pgarray;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;

import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

public class Helper {

    public static boolean toBoolean(String s)
    {
        if (s != null)
        {
            s = s.trim();

            if (s.equalsIgnoreCase("t") || s.equalsIgnoreCase("true") || s.equals("1"))
                return true;

            if (s.equalsIgnoreCase("f") || s.equalsIgnoreCase("false") || s.equals("0"))
                return false;

            try
            {
                if (Double.valueOf(s).doubleValue() == 1)
                    return true;
            }
            catch (NumberFormatException e)
            {
            }
        }
        return false;  // SQL NULL
    }

    private static final BigInteger INTMAX = new BigInteger(Integer.toString(Integer.MAX_VALUE));
    private static final BigInteger INTMIN = new BigInteger(Integer.toString(Integer.MIN_VALUE));

    public static int toInt(String s) throws SQLException
    {
        if (s != null)
        {
            try
            {
                s = s.trim();
                return Integer.parseInt(s);
            }
            catch (NumberFormatException e)
            {
                try
                {
                    BigDecimal n = new BigDecimal(s);
                    BigInteger i = n.toBigInteger();

                    int gt = i.compareTo(INTMAX);
                    int lt = i.compareTo(INTMIN);

                    if (gt > 0 || lt < 0)
                    {
                        throw new PSQLException(GT.tr("Bad value for type {0} : {1}", new Object[]{"int",s}),
                                                PSQLState.NUMERIC_VALUE_OUT_OF_RANGE);
                    }
                    return i.intValue();

                }
                catch ( NumberFormatException ne )
                {
                    throw new PSQLException(GT.tr("Bad value for type {0} : {1}", new Object[]{"int",s}),
                                            PSQLState.NUMERIC_VALUE_OUT_OF_RANGE);
                }
            }
        }
        return 0;  // SQL NULL
    }
    private final static BigInteger LONGMAX = new BigInteger(Long.toString(Long.MAX_VALUE));
    private final static BigInteger LONGMIN = new BigInteger(Long.toString(Long.MIN_VALUE));

    public static long toLong(String s) throws SQLException
    {
        if (s != null)
        {
            try
            {
                s = s.trim();
                return Long.parseLong(s);
            }
            catch (NumberFormatException e)
            {
                try
                {
                    BigDecimal n = new BigDecimal(s);
                    BigInteger i = n.toBigInteger();
                    int gt = i.compareTo(LONGMAX);
                    int lt = i.compareTo(LONGMIN);

                    if ( gt > 0 || lt < 0 )
                    {
                        throw new PSQLException(GT.tr("Bad value for type {0} : {1}", new Object[]{"long",s}),
                                                PSQLState.NUMERIC_VALUE_OUT_OF_RANGE);
                    }
                    return i.longValue();
                }
                catch ( NumberFormatException ne )
                {
                    throw new PSQLException(GT.tr("Bad value for type {0} : {1}", new Object[]{"long",s}),
                                            PSQLState.NUMERIC_VALUE_OUT_OF_RANGE);
                }
            }
        }
        return 0;  // SQL NULL
    }
	
    public static BigDecimal toBigDecimal(String s, int scale) throws SQLException
    {
        BigDecimal val;
        if (s != null)
        {
            try
            {
                s = s.trim();
                val = new BigDecimal(s);
            }
            catch (NumberFormatException e)
            {
                throw new PSQLException(GT.tr("Bad value for type {0} : {1}", new Object[]{"BigDecimal",s}),
                                        PSQLState.NUMERIC_VALUE_OUT_OF_RANGE);
            }
            if (scale == -1)
                return val;
            try
            {
                return val.setScale(scale);
            }
            catch (ArithmeticException e)
            {
                throw new PSQLException(GT.tr("Bad value for type {0} : {1}", new Object[]{"BigDecimal",s}),
                                        PSQLState.NUMERIC_VALUE_OUT_OF_RANGE);
            }
        }
        return null;  // SQL NULL
    }

    public static float toFloat(String s) throws SQLException
    {
        if (s != null)
        {
            try
            {
                s = s.trim();
                return Float.parseFloat(s);
            }
            catch (NumberFormatException e)
            {
                throw new PSQLException(GT.tr("Bad value for type {0} : {1}", new Object[]{"float",s}),
                                        PSQLState.NUMERIC_VALUE_OUT_OF_RANGE);
            }
        }
        return 0;  // SQL NULL
    }

    public static double toDouble(String s) throws SQLException
    {
        if (s != null)
        {
            try
            {
                s = s.trim();
                return Double.parseDouble(s);
            }
            catch (NumberFormatException e)
            {
                throw new PSQLException(GT.tr("Bad value for type {0} : {1}", new Object[]{"double",s}),
                                        PSQLState.NUMERIC_VALUE_OUT_OF_RANGE);
            }
        }
        return 0;  // SQL NULL
    }
	
}
