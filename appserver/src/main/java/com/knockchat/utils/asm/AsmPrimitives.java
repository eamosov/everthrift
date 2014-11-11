package com.knockchat.utils.asm;

import static org.objectweb.asm.Opcodes.CHECKCAST;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;
import static org.objectweb.asm.Opcodes.INVOKEVIRTUAL;

import org.objectweb.asm.MethodVisitor;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class AsmPrimitives {

	public static void box( MethodVisitor mv, Class<?> primitive ) {
		if ( primitive.equals( Byte.TYPE ) ) {
			mv.visitMethodInsn( INVOKESTATIC, "java/lang/Byte", "valueOf", "(B)Ljava/lang/Byte;" );
		} else if ( primitive.equals( Short.TYPE ) ) {
			mv.visitMethodInsn( INVOKESTATIC, "java/lang/Short", "valueOf", "(S)Ljava/lang/Short;" );
		} else if ( primitive.equals( Integer.TYPE ) ) {
			mv.visitMethodInsn( INVOKESTATIC, "java/lang/Integer", "valueOf", "(I)Ljava/lang/Integer;" );
		} else if ( primitive.equals( Long.TYPE ) ) {
			mv.visitMethodInsn( INVOKESTATIC, "java/lang/Long", "valueOf", "(J)Ljava/lang/Long;" );
		} else if ( primitive.equals( Float.TYPE ) ) {
			mv.visitMethodInsn( INVOKESTATIC, "java/lang/Float", "valueOf", "(F)Ljava/lang/Float;" );
		} else if ( primitive.equals( Double.TYPE ) ) {
			mv.visitMethodInsn( INVOKESTATIC, "java/lang/Double", "valueOf", "(D)Ljava/lang/Double;" );
		} else if ( primitive.equals( Boolean.TYPE ) ) {
			mv.visitMethodInsn( INVOKESTATIC, "java/lang/Boolean", "valueOf", "(Z)Ljava/lang/Boolean;" );
		} else if ( primitive.equals( Character.TYPE ) ) {
			mv.visitMethodInsn( INVOKESTATIC, "java/lang/Character", "valueOf", "(C)Ljava/lang/Character;" );
		} else if ( !primitive.isPrimitive() ) {
			throw new Error( "Cann't box non-primitive type " + primitive.getName() );
		} else {
			throw new Error( "Unknown primitive type " + primitive.getName() );
		}
	}

	public static void unbox( MethodVisitor mv, Class<?> primitive ) {
		if ( primitive.equals( Byte.TYPE ) ) {
			mv.visitTypeInsn( CHECKCAST, "java/lang/Number" );
			mv.visitMethodInsn( INVOKEVIRTUAL, "java/lang/Number", "byteValue", "()B" );
		} else if ( primitive.equals( Short.TYPE ) ) {
			mv.visitTypeInsn( CHECKCAST, "java/lang/Number" );
			mv.visitMethodInsn( INVOKEVIRTUAL, "java/lang/Number", "shortValue", "()S" );
		} else if ( primitive.equals( Integer.TYPE ) ) {
			mv.visitTypeInsn( CHECKCAST, "java/lang/Number" );
			mv.visitMethodInsn( INVOKEVIRTUAL, "java/lang/Number", "intValue", "()I" );
		} else if ( primitive.equals( Long.TYPE ) ) {
			mv.visitTypeInsn( CHECKCAST, "java/lang/Number" );
			mv.visitMethodInsn( INVOKEVIRTUAL, "java/lang/Number", "longValue", "()J" );
		} else if ( primitive.equals( Float.TYPE ) ) {
			mv.visitTypeInsn( CHECKCAST, "java/lang/Number" );
			mv.visitMethodInsn( INVOKEVIRTUAL, "java/lang/Number", "floatValue", "()F" );
		} else if ( primitive.equals( Double.TYPE ) ) {
			mv.visitTypeInsn( CHECKCAST, "java/lang/Number" );
			mv.visitMethodInsn( INVOKEVIRTUAL, "java/lang/Number", "doubleValue", "()D" );
		} else if ( primitive.equals( Boolean.TYPE ) ) {
			mv.visitTypeInsn( CHECKCAST, "java/lang/Boolean" );
			mv.visitMethodInsn( INVOKEVIRTUAL, "java/lang/Boolean", "booleanValue", "()Z" );
		} else if ( primitive.equals( Character.TYPE ) ) {
			mv.visitTypeInsn( CHECKCAST, "java/lang/Character" );
			mv.visitMethodInsn( INVOKEVIRTUAL, "java/lang/Character", "charValue", "()C" );
		} else if ( !primitive.isPrimitive() ) {
			throw new Error( "Cann't unbox to non-primitive type " + primitive.getName() );
		} else {
			throw new Error( "Unknown primitive type " + primitive.getName() );
		}
	}
}
