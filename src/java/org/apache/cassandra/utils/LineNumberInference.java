/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.io.Files;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.objectweb.asm.*;

/**
 * This class is responsible for retrieving a line number from the compiled bytecode for lambdas, named and
 * anonymous abstract class and interface and implementations.
 *
 * While in the most of cases the line number can be inferred directly from the bytecode, this is not the
 * case for Lambdas. Lambdas are compiled as a private static method in the bytecode (which also holds their
 * precise location - a line number). However, in runtime Lambdas are represented as a proxy class. Unfortunately,
 * there is no direct mapping between the proxy class and the static method. In order to establish this
 * mapping, we have to dump proxy classes generated in the runtime, which will will give both the name of the
 * proxy class instance during the runtime and the name of the private static method representing the lambda.
 */
public final class LineNumberInference
{
    public static final Descriptor UNKNOWN_SOURCE = new Descriptor("", "unknown", -1);

    private static final Logger logger = LoggerFactory.getLogger(LineNumberInference.class);
    private static final String INIT = "<init>";
    private static final Pattern LAMBDA_PATTERN = Pattern.compile("(.*)\\$\\$(Lambda)\\$(\\d+)/(\\d+)");
    private static final String DUMP_CLASSES = "jdk.internal.lambda.dumpProxyClasses";
    private final static String tmpDir;

    static
    {
        String setTmpDir = System.getProperty(DUMP_CLASSES);
        if (setTmpDir == null)
        {
            setTmpDir = Files.createTempDir().getAbsolutePath();
            // This approach does *NOT* work with Java9 and newer, because the system property has already been
            // evaluated and since it was not set, no classes are dumped. The evaluation of that property happens
            // during the initialization of the JVM, it has to be passed on the command line!
            logger.warn("Lambda line number inference not available");
            System.setProperty(DUMP_CLASSES, setTmpDir);
        }
        else
        {
            File dir = new File(setTmpDir);
            if (!dir.exists())
                dir.mkdirs();
            if (!dir.canWrite() || !dir.canRead() || !dir.isDirectory())
                throw new RuntimeException("Cannot use line number inference using directory " + dir.getAbsolutePath() + " since it's not readable/writable");
        }
        tmpDir = setTmpDir;

        logger.debug("Saving class files to {}", tmpDir);
    }

    // Initialisation is already done in the static context; method is no-op and shouold be used for referencing the class
    public static void init()
    {
        // no-op
    }

    /**
     * Contains the source descriptors for lambda proxy classes.
     */
    private final Map<Class<?>, Descriptor> descriptors;

    private boolean preloaded;

    /**
     * Constructor allowing filtering of the classes for which the preloading has to be done. Used to reduce
     * the runtime memory footprint of stack trace inference.
     */
    public LineNumberInference()
    {
        descriptors = new ConcurrentHashMap<>();
    }

    /**
     * Preloads the bytecode of the proxy classes for lambdas, dumped by JVM into the specified location.
     *
     * This is required to establish the mapping from the lambda class name to the actual method that
     * it's calling.
     */
    public void preloadLambdas()
    {
        if (preloaded)
            return;

        preloaded = true;
        try
        {
            final PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:**.class");

            Path tmpFilePath = new File(tmpDir).toPath();
            String lookupRegex = tmpFilePath + "\\/(.*)\\.class";

            Collection<File> files = java.nio.file.Files.find(tmpFilePath, 999, (path, attributes) -> matcher.matches(path))
                                                        .map(Path::toFile)
                                                        .collect(Collectors.toList());

            for (File file : files)
            {
                String className = slashToDot(file.getAbsolutePath().replaceFirst(lookupRegex, "$1"));

                try (InputStream in = new FileInputStream(file))
                {
                    LambdaClassVisitor clsVisitor = new LambdaClassVisitor();
                    new ClassReader(in).accept(clsVisitor, ClassReader.SKIP_FRAMES);

                    try
                    {
                        Class<?> klass = Class.forName(className, false, getClass().getClassLoader());
                        processLambdaImplementationMethod(klass, clsVisitor);
                    }
                    catch (ClassNotFoundException cnf)
                    {
                        // ignore during preload
                    }
                }
                catch (IOException e)
                {
                    logger.warn("Failed to preload lambdas: rich stack traces for flows will be disabled. ", e); // Check `dumpProxyClasses` property precedence: it has to be set before lambda loads
                }
            }
        }
        catch (Throwable e)
        {
            logger.warn("Couldn't load the list of lambdas for rich stack traces.", e);
        }
    }

    /**
     * Returns the Pair of Filename and Line Number where the given class was originally declared.
     */
    public Descriptor getLine(Class klass)
    {
        Descriptor descriptor = descriptors.get(klass);
        if (descriptor == null)
        {
            if (maybeProcessClassInternal(klass))
                descriptor = descriptors.get(klass);

            if (descriptor == null)
            {
                logger.trace("Could not find line information for " + klass);
                return UNKNOWN_SOURCE;
            }
        }

        return descriptor;
    }

    /**
     * Loads and extracts the line numbers from the bytecode of the class. In case of lambda instance
     * will load all the lambdas in the file at once.
     */
    boolean maybeProcessClass(Class klass)
    {
        try
        {
            if (descriptors.containsKey(klass))
                return false;

            return maybeProcessClassInternal(klass);
        }
        catch (Throwable e)
        {
            descriptors.put(klass, UNKNOWN_SOURCE);

            logger.debug("Could not process class information for lambda {}", klass.getName(), e);
            logger.warn("Could not process class information for lambda {}", klass.getName());
            return false;
        }
    }

    private boolean maybeProcessClassInternal(Class klass)
    {
        String className = klass.getName();
        try
        {
            // no need to filter by interface here since we're calling it from the framework level
            if (LAMBDA_PATTERN.matcher(className).matches())
            {
                String parentClass = className.split("/")[0];

                // Scan the lambda proxy class to extract the reference to the implementation method.
                LambdaClassVisitor clsVisitor = new LambdaClassVisitor();
                try (InputStream in = new FileInputStream(tmpDir + '/' + dotToSlash(parentClass) + ".class"))
                {
                    new ClassReader(in).accept(clsVisitor, ClassReader.SKIP_FRAMES);
                }

                processLambdaImplementationMethod(klass, clsVisitor);
            }
            else
            {
                // Nested classes, unlike lambdas are stored in the separate files and their line information is not
                // accessible through their enclosing class bytecode
                try (InputStream in = klass.getResourceAsStream('/' + dotToSlash(className) + ".class"))
                {
                    SimpleClassVisitor visitor = new SimpleClassVisitor();
                    new ClassReader(in).accept(visitor, ClassReader.SKIP_FRAMES);
                    descriptors.put(klass, visitor.descriptor());
                }
            }

            return true;
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to process class " + className, e);
        }
    }

    private void processLambdaImplementationMethod(Class klass, LambdaClassVisitor ref) throws IOException
    {
        // Scan the implementation class and inspect the lambda implementation method.
        String resourceName = '/' + dotToSlash(ref.lambdaClass) + ".class";
        try (InputStream in = klass.getResourceAsStream(resourceName))
        {
            LambdaImplementationVisitor implVisitor = new LambdaImplementationVisitor(klass, ref);
            new ClassReader(in).accept(implVisitor, ClassReader.SKIP_FRAMES);
            descriptors.put(klass, implVisitor.descriptor());
        }
    }

    /**
     * Bytecode Visitor for concrete and abstract classes and interface implementations
     */
    private static final class SimpleClassVisitor extends ClassVisitor
    {
        private String className;
        private String fileName;
        private int initLine = -1;
        private int methodLine = -1;

        SimpleClassVisitor()
        {
            super(Opcodes.ASM5);
        }

        @Override
        public void visit(int version,
                          int access,
                          String name,
                          String signature,
                          String superName,
                          String[] interfaces)
        {
            this.className = slashToDot(name);
        }

        public Descriptor descriptor()
        {
            return new Descriptor(className, fileName, methodLine != -1 ? methodLine : initLine);
        }

        @Override
        public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions)
        {
            if (name.equals(INIT))
            {
                return new MethodVisitor(Opcodes.ASM5)
                {
                    public void visitLineNumber(int line, Label label)
                    {
                        initLine = line;
                    }
                };
            }

            return new MethodVisitor(Opcodes.ASM5)
            {
                public void visitLineNumber(int line, Label label)
                {
                    // lower line numbers are synthetic methods
                    methodLine = Math.max(methodLine, line);
                }
            };
        }

        @Override
        public void visitSource(String fileName, String var2)
        {
            this.fileName = fileName;
        }
    }

    /**
     * Visitor for proxy classfiles generated by Java for lambdas, used to build a mapping from the
     * lambda proxy class to the private static method generated, since the runtime only has access to the proxy
     * class and bytecode visitor fetching line numbers only has access to the generated "fake" method.
     */
    private static class LambdaClassVisitor extends ClassVisitor
    {
        private String lambdaClass;
        private String lambdaMethod;
        private String lambdaMethodSignature;

        LambdaClassVisitor()
        {
            super(Opcodes.ASM5);
        }

        @Override
        public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions)
        {
            // For the abstract classes, the "real" will not be visited, since it's implemented in the other class.
            // In this situation the best thing we can do is to direct to record the base class constructor line number.
            if (name.equals(INIT))
                return null;

            return new MethodVisitor(Opcodes.ASM5)
            {
                public void visitMethodInsn(int opcode, String path, String methodRef, String methodSignature, boolean b)
                {
                    lambdaClass = slashToDot(path);
                    lambdaMethod = methodRef;
                    lambdaMethodSignature = methodSignature;
                }
            };
        }
    }

    /**
     * Class visitor for the class that implements the lambda method. This is not the lambda class but the class
     * that declares the lambda.
     */
    private static class LambdaImplementationVisitor extends ClassVisitor
    {
        // Assigned from outside
        private final Class<?> klass;
        private final LambdaClassVisitor methodReference;

        private int initLine = -1;
        private int sourceLine = -1;
        private String sourceFileName;

        LambdaImplementationVisitor(Class<?> klass, LambdaClassVisitor methodReference)
        {
            super(Opcodes.ASM5);
            this.klass = klass;
            this.methodReference = methodReference;
        }

        Descriptor descriptor()
        {
            int line = sourceLine != -1 ? sourceLine : initLine;
            return line != -1 && sourceFileName != null
                   ? new Descriptor(klass.getName(), sourceFileName, line)
                   : UNKNOWN_SOURCE;
        }

        @Override
        public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions)
        {
            // For the abstract classes, the "real" will not be visited, since it's implemented in the other class.
            // In this situation the best thing we can do is to direct to record the base class constructor line number.
            if (name.equals(INIT))
            {
                return new MethodVisitor(Opcodes.ASM5)
                {
                    public void visitLineNumber(int line, Label label)
                    {
                        if (initLine == -1)
                            initLine = line;
                    }
                };
            }

            // Filter out currently queried method
            if (!name.equals(methodReference.lambdaMethod) || !desc.equals(methodReference.lambdaMethodSignature))
                return null;

            return new MethodVisitor(Opcodes.ASM5)
            {
                public void visitLineNumber(int line, Label label)
                {
                    if (sourceLine == -1)
                        sourceLine = line;
                }
            };
        }

        @Override
        public void visitSource(String fileName, String var2)
        {
            this.sourceFileName = fileName;
        }
    }

    private static String dotToSlash(String input)
    {
        return input.replace('.', '/');
    }

    private static String slashToDot(String input)
    {
        return input.replace('/', '.');
    }

    public static final class Descriptor
    {
        final String className;
        final String sourceFilePath;
        final int line;

        Descriptor(String className, String sourceFilePath, int line)
        {
            this.className = className;
            this.sourceFilePath = sourceFilePath;
            this.line = line;
        }

        public int line()
        {
            return line;
        }

        public String source()
        {
            return sourceFilePath;
        }

        public String toString()
        {
            return "ClassDescriptor{" +
                   ", source='" + sourceFilePath + '\'' +
                   ", line=" + line +
                   '}';
        }
    }
}
