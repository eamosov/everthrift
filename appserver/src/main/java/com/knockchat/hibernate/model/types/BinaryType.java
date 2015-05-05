package com.knockchat.hibernate.model.types;

import java.util.Comparator;

import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.type.AbstractSingleColumnStandardBasicType;
import org.hibernate.type.VersionType;
import org.hibernate.type.descriptor.java.PrimitiveByteArrayTypeDescriptor;
import org.hibernate.type.descriptor.sql.BinaryTypeDescriptor;

/**
 * A type that maps between a {@link java.sql.Types#VARBINARY VARBINARY} and {@code byte[]}
 *
 * @author Gavin King
 * @author Steve Ebersole
 */
public class BinaryType
		extends AbstractSingleColumnStandardBasicType<byte[]>
		implements VersionType<byte[]> {

	public static final BinaryType INSTANCE = new BinaryType();

	@Override
	public String getName() {
		return "binary";
	}

	public BinaryType() {
		super( BinaryTypeDescriptor.INSTANCE, PrimitiveByteArrayTypeDescriptor.INSTANCE );
	}

	@Override
	public String[] getRegistrationKeys() {
		return new String[] { getName(), "byte[]", byte[].class.getName() };
	}

	@Override
	public byte[] seed(SessionImplementor session) {
		// Note : simply returns null for seed() and next() as the only known
		// 		application of binary types for versioning is for use with the
		// 		TIMESTAMP datatype supported by Sybase and SQL Server, which
		// 		are completely db-generated values...
		return null;
	}

	@Override
	public byte[] next(byte[] current, SessionImplementor session) {
		return current;
	}

	@Override
	public Comparator<byte[]> getComparator() {
		return PrimitiveByteArrayTypeDescriptor.INSTANCE.getComparator();
	}
}