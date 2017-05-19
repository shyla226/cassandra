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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;

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
    private static final Logger logger = LoggerFactory.getLogger(LineNumberInference.class);
    private static final Pattern DOT = Pattern.compile(".", Pattern.LITERAL);
    private static final Pattern SLASH = Pattern.compile("/", Pattern.LITERAL);
    private static final String INIT = "<init>";
    private static final Pair<String, Integer> UNKNOWN_SOURCE = Pair.create("unknown", -1);
    private static final Pattern LAMBDA_PATTERN = Pattern.compile("(.*)\\$\\$(Lambda)\\$(\\d+)/(\\d+)");


    public final static String tmpDir;
    static {
        tmpDir = Files.createTempDir().getAbsolutePath();
        System.setProperty("jdk.internal.lambda.dumpProxyClasses", tmpDir);
    }

    private final BiMap<String, Pair<String, String>> mappings;
    private final BiMap<Pair<String, String>, String> inverseMappings;
    private final Map<String, Pair<String, Integer>> lines;
    private final Set<String> processedFiles;
    private final Set<String> processedProxyClasses;
    private final Predicate<Class> matcher;
    private final LambdaClassVisitor lambdaClassVisitor = new LambdaClassVisitor();
    private final SimpleClassVisitor simpleClassVisitor = new SimpleClassVisitor();
    private final ReadWriteLock lock;
    private final Lock readLock;
    private final Lock writeLock;

    private volatile long lastModified;

    public LineNumberInference()
    {
        this(null);
    }

    /**
     * Constructor allowing filtering of the classes for which the preloading has to be done. Used to reduce
     * the runtime memory footprint of stack trace inference.
     */
    public LineNumberInference(Predicate<Class> matcher)
    {
        this.matcher = matcher;
        mappings = HashBiMap.create();
        inverseMappings = mappings.inverse();
        lines = Maps.newHashMap();
        processedFiles = new HashSet<>();
        processedProxyClasses = new HashSet<>();
        lastModified = Long.MIN_VALUE;
        lock = new ReentrantReadWriteLock();
        readLock = lock.readLock();
        writeLock = lock.writeLock();
    }

    /**
     * Preloads the bytecode of the proxy classes for lambdas, dumped by JVM into the specified location.
     */
    public void preloadLambdas()
    {
        File dirFile = new File(tmpDir);
        long newLastModified = dirFile.lastModified();

        if (newLastModified > lastModified)
        {
            writeLock.lock();
            try
            {
                lastModified = newLastModified;
                for (File file : FileUtils.listFiles(dirFile, new String[]{ "class" }, true))
                {
                    if (processedProxyClasses.contains(file.getAbsolutePath()))
                        continue;

                    try (InputStream in = new FileInputStream(file))
                    {
                        new ClassReader(in).accept(new FileMappingVisitor(), ClassReader.EXPAND_FRAMES);
                        processedProxyClasses.add(file.getAbsolutePath());
                    }
                    catch (IOException e)
                    {
                        logger.warn("Failed to preload lambdas: rich stack traces for flows will be disabled", e);
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
    }

    /**
     * Returns the Pair of Filename and Line Number where the given class was originally declared.
     */
    public Pair<String, Integer> getLine(Class klass)
    {
        readLock.lock();
        try
        {
            Pair<String, Integer> line = lines.get(klass.getName().split("/")[0]);
            if (line == null)
            {
                logger.info("Could not find line information for " + klass);
                return UNKNOWN_SOURCE;
            }

            return line;
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
    public void maybeProcessClass(Class klass)
    {
        writeLock.lock();
        try
        {
            maybeProcessClassInternal(klass);
        }
        catch (Throwable e)
        {
            logger.warn("Could not process class information for lambda " + klass.getName());
        }
        finally
        {
            writeLock.unlock();
        }
    }

    private void maybeProcessClassInternal(Class klass) throws IOException
    {
        String className = klass.getName();
        // no need to filter by interface here since we're calling it from the framework level
        if (LAMBDA_PATTERN.matcher(className).matches())
        {
            String parentClass = className.split("/")[0];
            if (processedFiles.contains(parentClass))
                return;

            Pair<String, String> methodDescriptor = mappings.get(parentClass);

            // Skipped lambda, for which no mapping is found
            if (methodDescriptor == null)
                return;

            try (InputStream in = klass.getResourceAsStream("/" + methodDescriptor.left + ".class"))
            {
                new ClassReader(in).accept(lambdaClassVisitor, ClassReader.SKIP_FRAMES);
            }

            processedFiles.add(parentClass);
        }
        else
        {
            if (processedFiles.contains(className))
                return;

            // Nested classes, unlike lambdas are stored in the separate files and their line information is not
            // accessible through their enclosing class bytecode
            try (InputStream in = klass.getResourceAsStream("/" + dotToSlash(className) + ".class"))
            {
                new ClassReader(in).accept(simpleClassVisitor, ClassReader.SKIP_FRAMES);
            }

            processedFiles.add(klass.getName());
        }
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
                        lines.put(className, Pair.create(fileName, line));
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
        private String className;
        private String fileName;

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
            this.className = name;
        }

        @Override
        public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions)
        {
            String pendingMethod = inverseMappings.get(Pair.create(this.className, name));

            // Skip unmapped lambdas: they either weren't loaded or did't match the interface
            if (pendingMethod == null)
                return null;

            return new MethodVisitor(Opcodes.ASM5)
            {
                public void visitLineNumber(int line, Label label)
                {
                    lines.put(pendingMethod, Pair.create(fileName, line));
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
     * Visitor for proxy classfiles generated by Java for lambdas, used to build a bi-directional mapping from the
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
        private boolean process;

        @Override
        public void visit(int version,
                          int access,
                          String name,
                          String signature,
                          String superName,
                          String[] interfaces)
        {
            this.currentLambda = slashToDot(name);

            if (matcher == null)
                process = false;
            else if (interfaces.length == 0)
                process = true;
            else
            {
                process = Arrays.stream(interfaces)
                                .map(LineNumberInference.this::slashToDot)
                                .map(this::safeClassForName)
                                .filter(Objects::nonNull)
                                .reduce(false, (acc, klass) -> acc || matcher.test(klass), (l, r) -> l || r);
            }
        }

        @Override
        public MethodVisitor visitMethod(int access, String methodName, String desc, String signature, String[] exceptions)
        {
            if (!process)
            {
                return null;
            }

            if (methodName.equals(INIT))
                return null;

            return new MethodVisitor(Opcodes.ASM5)
            {
                public void visitMethodInsn(int opcode, String path, String methodRef, String s2, boolean b)
                {
                    // lambda proxies are calling the generated private static method
                    if (Opcodes.INVOKESTATIC == opcode || (Opcodes.INVOKESPECIAL == opcode && !methodRef.equals(INIT)))
                    {
                        mappings.put(currentLambda, Pair.create(path, methodRef));
                    }
                }
            };
        }

        private Class<?> safeClassForName(String iface)
        {
            try
            {
                return Class.forName(iface);
            }
            catch (ClassNotFoundException e)
            {
                return null;
            }
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
}
