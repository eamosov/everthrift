package org.everthrift.sql.hibernate.model.types;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.type.AbstractSingleColumnStandardBasicType;
import org.hibernate.type.VersionType;
import org.hibernate.type.descriptor.java.PrimitiveByteArrayTypeDescriptor;
import org.hibernate.type.descriptor.sql.BinaryTypeDescriptor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;

/**
 * A type that maps between a {@link java.sql.Types#VARBINARY VARBINARY} and
 * {@code byte[]}
 *
 * @author Gavin King
 * @author Steve Ebersole
 */
public class BinaryType extends AbstractSingleColumnStandardBasicType<byte[]> implements VersionType<byte[]> {

    public static final BinaryType INSTANCE = new BinaryType();

    @NotNull
    @Override
    public String getName() {
        return "binary";
    }

    public BinaryType() {
        super(BinaryTypeDescriptor.INSTANCE, PrimitiveByteArrayTypeDescriptor.INSTANCE);
    }

    @NotNull
    @Override
    public String[] getRegistrationKeys() {
        return new String[]{getName(), "byte[]", byte[].class.getName()};
    }

    @Nullable
    @Override
    public byte[] seed(SharedSessionContractImplementor session) {
        // Note : simply returns null for seed() and next() as the only known
        // application of binary types for versioning is for use with the
        // TIMESTAMP datatype supported by Sybase and SQL Server, which
        // are completely db-generated values...
        return null;
    }

    @Override
    public byte[] next(byte[] current, SharedSessionContractImplementor session) {
        return current;
    }

    @Override
    public Comparator<byte[]> getComparator() {
        return PrimitiveByteArrayTypeDescriptor.INSTANCE.getComparator();
    }
}