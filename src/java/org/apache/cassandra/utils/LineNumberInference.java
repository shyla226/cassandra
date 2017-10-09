/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

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
    public static final Descriptor UNKNOWN_SOURCE = new Descriptor()
    {
        public int line()
        {
            return -1;
        }

        public String source()
        {
            return "unknown";
        }
    };

    private static final Logger logger = LoggerFactory.getLogger(LineNumberInference.class);
    private static final Pattern DOT = Pattern.compile(".", Pattern.LITERAL);
    private static final Pattern SLASH = Pattern.compile("/", Pattern.LITERAL);
    private static final String INIT = "<init>";
    private static final Pattern LAMBDA_PATTERN = Pattern.compile("(.*)\\$\\$(Lambda)\\$(\\d+)/(\\d+)");
    private static final String DUMP_CLASSES = "jdk.internal.lambda.dumpProxyClasses";
    private final static String tmpDir = Files.createTempDir().getAbsolutePath();

    static
    {
        logger.debug("Saving class files to {}", tmpDir);
        System.setProperty(DUMP_CLASSES, tmpDir);
    }

    // Initialisation is already done in the static context; method is no-op and shouold be used for referencing the class
    public static void init()
    {
        // no-op
    }

    // Maps the internal lambda class name to the descriptor
    final Map<String, Descriptor> mappings;
    private final LambdaClassVisitor lambdaClassVisitor = new LambdaClassVisitor();
    private final SimpleClassVisitor simpleClassVisitor = new SimpleClassVisitor();
    private final ReadWriteLock lock;
    private final Lock readLock;
    private final Lock writeLock;
    private final Path tmpFilePath;
    private final String lookupRegex;

    /**
     * Constructor allowing filtering of the classes for which the preloading has to be done. Used to reduce
     * the runtime memory footprint of stack trace inference.
     */
    public LineNumberInference()
    {
        mappings = new HashMap<>();
        lock = new ReentrantReadWriteLock();
        readLock = lock.readLock();
        writeLock = lock.writeLock();
        tmpFilePath = new File(tmpDir).toPath();
        lookupRegex = tmpFilePath + "\\/(.*)\\.class";
    }

    /**
     * Preloads the bytecode of the proxy classes for lambdas, dumped by JVM into the specified location.
     *
     * This is required to establish the mapping from the lambda class name to the actual method that
     * it's calling.
     */
    public void preloadLambdas()
    {
        writeLock.lock();
        try
        {
            final PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:**.class");

            Collection<File> files = java.nio.file.Files.find(tmpFilePath, 999, (path, attributes) -> matcher.matches(path))
                                                        .map(Path::toFile)
                                                        .collect(Collectors.toList());

            for (File file : files)
            {
                 String className = slashToDot(file.getAbsolutePath().replaceFirst(lookupRegex, "$1"));

                 // Already processed
                if (mappings.containsKey(className))
                    continue;

                try (InputStream in = new FileInputStream(file))
                {
                    new ClassReader(in).accept(new FileMappingVisitor(), ClassReader.EXPAND_FRAMES);
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
        finally
        {
            writeLock.unlock();
        }
    }

    /**
     * Returns the Pair of Filename and Line Number where the given class was originally declared.
     */
    public Descriptor getLine(Class klass)
    {
        readLock.lock();
        try
        {
            String klassName = klass.getName().split("/")[0];
            Descriptor descriptor = mappings.get(klassName);
            if (descriptor == null)
            {
                logger.trace("Could not find line information for " + klass);
                return UNKNOWN_SOURCE;
            }

            return descriptor;
        }
        finally
        {
            readLock.unlock();
        }
    }

    /**
     * Loads and extracts the line numbers from the bytecode of the class. In case of lambda instance
     * will load all the lambdas in the file at once.
     */
    public boolean maybeProcessClass(Class klass)
    {
        writeLock.lock();
        try
        {
            return maybeProcessClassInternal(klass);
        }
        catch (Throwable e)
        {
            logger.warn("Could not process class information for lambda " + klass.getName());
            return false;
        }
        finally
        {
            writeLock.unlock();
        }
    }

    private boolean maybeProcessClassInternal(Class klass) throws IOException
    {
        String className = klass.getName();
        // no need to filter by interface here since we're calling it from the framework level
        if (LAMBDA_PATTERN.matcher(className).matches())
        {
            String parentClass = className.split("/")[0];

            LambdaDescriptor lambdaDescriptor = (LambdaDescriptor) mappings.get(parentClass);
            if (lambdaDescriptor.line != -1)
                return false;

            try (InputStream in = klass.getResourceAsStream("/" + lambdaDescriptor.classFilePath + ".class"))
            {
                lambdaClassVisitor.lambdaDescriptor = lambdaDescriptor;
                new ClassReader(in).accept(lambdaClassVisitor, ClassReader.SKIP_FRAMES);
                lambdaClassVisitor.lambdaDescriptor = null;
            }
        }
        else
        {
            if (mappings.containsKey(className))
                return false;

            // Nested classes, unlike lambdas are stored in the separate files and their line information is not
            // accessible through their enclosing class bytecode
            try (InputStream in = klass.getResourceAsStream("/" + dotToSlash(className) + ".class"))
            {
                new ClassReader(in).accept(simpleClassVisitor, ClassReader.SKIP_FRAMES);
            }
        }

        return true;
     }

    /**
     * Bytecode Visitor for concrete and abstract classes and interface implementations
     */
    private class SimpleClassVisitor extends ClassVisitor
    {
        private String className;
        private String fileName;

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
            this.className = SLASH.matcher(name).replaceAll(".");
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
                        mappings.put(className, new ClassDescriptor(className, fileName, line));
                    }
                };
            }

            return null;
        }

        @Override
        public void visitSource(String fileName, String var2)
        {
            this.fileName = fileName;
        }
    }

    /**
     * Bytecode Visitor for class the anonymous / lambda interface implementations
     */
    private class LambdaClassVisitor extends ClassVisitor
    {
        // Assigned from outside
        LambdaDescriptor lambdaDescriptor;
        String fileName;

        LambdaClassVisitor()
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
                        if (lambdaDescriptor.line == -1)
                            lambdaDescriptor.line = line;
                    }
                };
            }

            // Filter out currently queried method
            if (!name.equals(lambdaDescriptor.targetMethodRef))
                return null;

            return new MethodVisitor(Opcodes.ASM5)
            {
                public void visitLineNumber(int line, Label label)
                {
                    lambdaDescriptor.line = line;
                    lambdaDescriptor.sourceFilePath = fileName;
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
    private class FileMappingVisitor extends ClassVisitor
    {
        FileMappingVisitor()
        {
            super(Opcodes.ASM5);
        }

        // Name of currently visited lambda
        private String currentLambda;

        @Override
        public void visit(int version,
                          int access,
                          String name,
                          String signature,
                          String superName,
                          String[] interfaces)
        {
            this.currentLambda = slashToDot(name);
        }

        @Override
        public MethodVisitor visitMethod(int access, String methodName, String desc, String signature, String[] exceptions)
        {
            if (methodName.equals(INIT))
                return null;

            return new MethodVisitor(Opcodes.ASM5)
            {
                public void visitMethodInsn(int opcode, String path, String methodRef, String s2, boolean b)
                {
                    mappings.put(currentLambda, new LambdaDescriptor(currentLambda, path, methodRef));
                }
            };
        }
    }

    private String dotToSlash(String input)
    {
        return DOT.matcher(input).replaceAll("/");
    }

    private String slashToDot(String input)
    {
        return SLASH.matcher(input).replaceAll(".");
    }

    public interface Descriptor
    {
        public int line();
        public String source();
    }

    private static class LambdaDescriptor implements Descriptor
    {
        final String className;
        final String classFilePath;
        final String targetMethodRef;

        String sourceFilePath = "unknown";
        int line = -1;

        LambdaDescriptor(String className, String classFilePath, String targetMethodRef)
        {
            this.className = className;
            this.classFilePath = classFilePath;
            this.targetMethodRef = targetMethodRef;
        }

        public String toString()
        {
            return "LambdaDescriptor{" +
                   ", source='" + sourceFilePath + '\'' +
                   ", line=" + line +
                   '}';
        }

        public int line()
        {
            return line;
        }

        public String source()
        {
            return sourceFilePath;
        }
    }

    public static class ClassDescriptor implements Descriptor
    {
        final String className;
        final String sourceFilePath;
        final int line;

        public ClassDescriptor(String className, String sourceFilePath, int line)
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
