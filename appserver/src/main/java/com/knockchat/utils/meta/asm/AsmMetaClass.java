package com.knockchat.utils.meta.asm;

import static com.knockchat.utils.JvmNotation.getJvmClass;
import static com.knockchat.utils.JvmNotation.getJvmSignature;
import static com.knockchat.utils.asm.AsmPrimitives.box;
import static com.knockchat.utils.asm.AsmPrimitives.unbox;
import static com.knockchat.utils.asm.AsmUtils.buildDefaultConstructor;
import static org.objectweb.asm.Opcodes.AALOAD;
import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ACONST_NULL;
import static org.objectweb.asm.Opcodes.ALOAD;
import static org.objectweb.asm.Opcodes.ARETURN;
import static org.objectweb.asm.Opcodes.BIPUSH;
import static org.objectweb.asm.Opcodes.CHECKCAST;
import static org.objectweb.asm.Opcodes.GETFIELD;
import static org.objectweb.asm.Opcodes.INVOKEVIRTUAL;
import static org.objectweb.asm.Opcodes.PUTFIELD;
import static org.objectweb.asm.Opcodes.RETURN;
import static org.objectweb.asm.Opcodes.V1_5;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.knockchat.utils.JvmNotation;
import com.knockchat.utils.meta.MetaMethod;
import com.knockchat.utils.meta.MetaProperty;
import com.knockchat.utils.meta.PreparedMetaClass;
import com.knockchat.utils.meta.getset.GetSetPropertySupport;


/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class AsmMetaClass extends PreparedMetaClass {

	@SuppressWarnings("unused")
	private static final Logger log = LoggerFactory.getLogger( AsmMetaClass.class );

	private static final String METAMETHOD_CLASSNAME = MetaMethod.class.getName().replace( '.', '/' );
	private static final String METAPROPERTY_CLASSNAME = MetaProperty.class.getName().replace( '.', '/' );

	protected final DefiningClassLoader classLoader;
	
	private final String baseJvmName;
	private final String baseClassName;

	public AsmMetaClass( Class<?> objectClass, DefiningClassLoader classLoader ) {
		super( objectClass );
		
		if ( objectClass.getName().startsWith( "java." ) ) {
			baseJvmName = "g_" + jvmName;
			baseClassName = "g_" + objectClass.getName();
		} else {
			baseJvmName = jvmName;
			baseClassName = objectClass.getName();
		}

		try {
			this.classLoader = classLoader;
	
			for ( Field field : objectClass.getFields() ) {
								
				if ( ( field.getModifiers() & Modifier.STATIC) != 0 )
					continue;

				if ( ( field.getModifiers() & Modifier.FINAL) != 0 )
					continue;

				fieldProperties.put( field.getName(), buildMetaField( field ) );
			}
	
			for ( Method method : objectClass.getMethods() ) {
				if ( ( method.getModifiers() & Modifier.STATIC) != 0 )
					continue;
				
				methods.put( method.getName(), buildMetaMethod( method ) );
			}
		} catch ( Throwable e ) {
			throw new Error("Can't create AsmMetaClass for class " + objectClass.getName(), e );
		}
		
		GetSetPropertySupport.get( this.getMethods(), beanProperties );
	}

	protected MetaMethod buildMetaMethod( Method method ) {
		String className = generateClassName( method );

		ClassWriter cw = new ClassWriter( ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS );
		cw.visit( V1_5, ACC_PUBLIC, baseJvmName + className, null, "java/lang/Object", new String[] { METAMETHOD_CLASSNAME } ); // Создаем класс

		buildDefaultConstructor( cw );
		buildInvokeMethod( cw, method );
		buildGetNameMethod( cw, method );
		buildGetReturnTypeMethod( cw, method );
		buildGetSignatureMethod( cw, method );

		cw.visitEnd();

		try {
			return (MetaMethod) classLoader.define( baseClassName + className, cw.toByteArray() ).newInstance();
		} catch ( Throwable e ) {
			throw new Error( "Error building metamethod " + objectClass.getName() + "#" + method.getName(), e );
		}
	}

	protected MetaProperty buildMetaField( Field field ) {
		String className = generateClassName( field );

		ClassWriter cw = new ClassWriter( ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS );
		cw.visit( V1_5, ACC_PUBLIC, baseJvmName + className, null, "java/lang/Object", new String[] { METAPROPERTY_CLASSNAME } ); // Создаем класс

		buildDefaultConstructor( cw );

		buildSetMethod( cw, field );
		buildGetMethod( cw, field );
		buildGetNameMethod( cw, field );
		buildGetTypeMethod( cw, field );

		cw.visitEnd();

		try {
			return (MetaProperty) classLoader.define( baseClassName + className, cw.toByteArray() ).newInstance();
		} catch ( Throwable e ) {
			throw new Error( "Error building metafield " + objectClass.getName() + "#" + field.getName(), e );
		}
	}

	private void buildGetReturnTypeMethod( ClassWriter cw, Method method ) {
		MethodVisitor mv = cw.visitMethod( ACC_PUBLIC, "getReturnType", "()Ljava/lang/Class;", null, null );
		mv.visitCode();

		appendClassConst( mv, method.getReturnType() );
		mv.visitInsn( ARETURN );

		mv.visitMaxs( 0, 0 );
		mv.visitEnd(); // Завершаем построение метода
	}

	private void buildGetNameMethod( ClassWriter cw, Method method ) {
		MethodVisitor mv = cw.visitMethod( ACC_PUBLIC, "getName", "()Ljava/lang/String;", null, null );
		mv.visitCode();

		mv.visitLdcInsn( method.getName() );
		mv.visitInsn( ARETURN );

		mv.visitMaxs( 0, 0 );
		mv.visitEnd(); // Завершаем построение метода
	}
	
	private void buildGetTypeMethod( ClassWriter cw, Field field ) {
		MethodVisitor mv = cw.visitMethod( ACC_PUBLIC, "getType", "()Ljava/lang/Class;", null, null );
		mv.visitCode();

		appendClassConst( mv, field.getType() );
		mv.visitInsn( ARETURN );

		mv.visitMaxs( 0, 0 );
		mv.visitEnd(); // Завершаем построение метода
	}

	private void buildGetNameMethod( ClassWriter cw, Field field ) {
		MethodVisitor mv = cw.visitMethod( ACC_PUBLIC, "getName", "()Ljava/lang/String;", null, null );
		mv.visitCode();

		mv.visitLdcInsn( field.getName() );
		mv.visitInsn( ARETURN );

		mv.visitMaxs( 0, 0 );
		mv.visitEnd(); // Завершаем построение метода
	}

	private void buildGetSignatureMethod( ClassWriter cw, Method method ) {
		MethodVisitor mv = cw.visitMethod( ACC_PUBLIC, "getSignature", "()Ljava/lang/String;", null, null );
		mv.visitCode();

		mv.visitLdcInsn( getJvmSignature( method ) );
		mv.visitInsn( ARETURN );

		mv.visitMaxs( 0, 0 );
		mv.visitEnd(); // Завершаем построение метода
	}

	private void buildGetMethod( ClassWriter cw, Field field ) {
		MethodVisitor mv = cw.visitMethod( ACC_PUBLIC, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", null, null );
		mv.visitCode();

		mv.visitVarInsn( ALOAD, 1 ); // $1 = (TargetType)target
		mv.visitTypeInsn( CHECKCAST, jvmName );

		mv.visitFieldInsn( GETFIELD, jvmName, field.getName(), JvmNotation.getJvmType( field.getType() ) );

		if ( field.getType().isPrimitive() )
			box( mv, field.getType() );

		mv.visitInsn( ARETURN );

		mv.visitMaxs( 0, 0 );
		mv.visitEnd(); // Завершаем построение метода
	}

	private void buildSetMethod( ClassWriter cw, Field field ) {
		MethodVisitor mv = cw.visitMethod( ACC_PUBLIC, "set", "(Ljava/lang/Object;Ljava/lang/Object;)V", null, null );
		mv.visitCode();

		mv.visitVarInsn( ALOAD, 1 ); // $1 = (TargetType)target
		mv.visitTypeInsn( CHECKCAST, jvmName );

		mv.visitVarInsn( ALOAD, 2 );

		if ( field.getType().isPrimitive() )
			unbox( mv, field.getType() );
		else
			mv.visitTypeInsn( CHECKCAST, getJvmClass( field.getType() ) );

		mv.visitFieldInsn( PUTFIELD, jvmName, field.getName(), JvmNotation.getJvmType( field.getType() ) );

		mv.visitInsn( RETURN );

		mv.visitMaxs( 0, 0 );
		mv.visitEnd(); // Завершаем построение метода
	}

	private void buildInvokeMethod( ClassWriter cw, Method method ) throws Error {
		MethodVisitor mv = cw.visitMethod( ACC_PUBLIC, "invoke", "(Ljava/lang/Object;[Ljava/lang/Object;)Ljava/lang/Object;", null, null );
		mv.visitCode();

		mv.visitVarInsn( ALOAD, 1 ); // $1 = (TargetType)target
		mv.visitTypeInsn( CHECKCAST, jvmName );

		/*
		 * Проходим по списку параметров, загружая их в стек и преобразуя к
		 * нужному типу. Примитивные типы необходимо получать путем анбоксинга.
		 */
		for ( int i = 0; i < method.getParameterTypes().length; ++i ) {
			Class<?> paramType = method.getParameterTypes()[i];

			mv.visitVarInsn( ALOAD, 2 ); // $2 = args
			mv.visitIntInsn( BIPUSH, i );
			mv.visitInsn( AALOAD ); // $3 = $2[i]

			if ( !paramType.isPrimitive() ) {
				mv.visitTypeInsn( CHECKCAST, paramType.getName().replace( '.', '/' ) );
			} else {
				unbox( mv, paramType );
			}
		}

		mv.visitMethodInsn( INVOKEVIRTUAL, jvmName, method.getName(), getJvmSignature( method ) ); // $1.method($3...)

		if ( method.getReturnType().equals( Void.TYPE ) ) {
			mv.visitInsn( ACONST_NULL );
			mv.visitInsn( ARETURN );
		} else {
			if ( method.getReturnType().isPrimitive() )
				box( mv, method.getReturnType() );

			mv.visitInsn( ARETURN );
		}

		mv.visitMaxs( 0, 0 );
		mv.visitEnd(); // Завершаем построение метода
	}

	private String generateClassName( Method method ) {
		return "$MetaMethod$" + method.getName() + "$" + classLoader.nextClassIndex();
	}

	private String generateClassName( Field field ) {
		return "$MetaField$" + field.getName() + "$" + classLoader.nextClassIndex();
	}

	private void appendClassConst( MethodVisitor mv, Class<?> type ) {
		if ( type.isPrimitive() ) {			
			if ( type.equals( Byte.TYPE ) ) {
				mv.visitFieldInsn( Opcodes.GETSTATIC, "java/lang/Byte", "TYPE", Type.getDescriptor( Class.class ) );
			} else if ( type.equals( Short.TYPE ) ) {
				mv.visitFieldInsn( Opcodes.GETSTATIC, "java/lang/Short", "TYPE", Type.getDescriptor( Class.class ) );
			} else if ( type.equals( Integer.TYPE ) ) {
				mv.visitFieldInsn( Opcodes.GETSTATIC, "java/lang/Integer", "TYPE", Type.getDescriptor( Class.class ) );
			} else if ( type.equals( Long.TYPE ) ) {
				mv.visitFieldInsn( Opcodes.GETSTATIC, "java/lang/Long", "TYPE", Type.getDescriptor( Class.class ) );
			} else if ( type.equals( Double.TYPE ) ) {
				mv.visitFieldInsn( Opcodes.GETSTATIC, "java/lang/Double", "TYPE", Type.getDescriptor( Class.class ) );
			} else if ( type.equals( Float.TYPE ) ) {
				mv.visitFieldInsn( Opcodes.GETSTATIC, "java/lang/Float", "TYPE", Type.getDescriptor( Class.class ) );
			} else if ( type.equals( Boolean.TYPE ) ) {
				mv.visitFieldInsn( Opcodes.GETSTATIC, "java/lang/Boolean", "TYPE", Type.getDescriptor( Class.class ) );
			} else if ( type.equals( Character.TYPE ) ) {
				mv.visitFieldInsn( Opcodes.GETSTATIC, "java/lang/Character", "TYPE", Type.getDescriptor( Class.class ) );
			} else if ( type.equals( Void.TYPE ) ) {
				mv.visitFieldInsn( Opcodes.GETSTATIC, "java/lang/Void", "TYPE", Type.getDescriptor( Class.class ) );
			} else {
				throw new RuntimeException("Unknown primitive type" + type.getName());
			}			
		} else {
			mv.visitLdcInsn( Type.getType( type ) );
		}
	}

}
