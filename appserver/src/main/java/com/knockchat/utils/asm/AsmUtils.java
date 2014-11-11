package com.knockchat.utils.asm;

import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ALOAD;
import static org.objectweb.asm.Opcodes.INVOKESPECIAL;
import static org.objectweb.asm.Opcodes.RETURN;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class AsmUtils {
	
	public static void buildDefaultConstructor( ClassVisitor cw ) {
		MethodVisitor mw = cw.visitMethod( ACC_PUBLIC, "<init>", "()V", null, null );
		mw.visitCode();
		mw.visitVarInsn( ALOAD, 0 );
		mw.visitMethodInsn( INVOKESPECIAL, "java/lang/Object", "<init>", "()V" );
		mw.visitInsn( RETURN );
		mw.visitMaxs( 0, 0 );
		mw.visitEnd();
	}
}
