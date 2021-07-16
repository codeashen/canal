package com.alibaba.otter.canal.connector.core.spi;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SPI 类加载器
 *
 * @author rewerma 2018-8-19 下午11:30:49
 * @version 1.0.0
 */
public class ExtensionLoader<T> {

    private static final Logger                                      logger                     = LoggerFactory.getLogger(ExtensionLoader.class);

    private static final String                                      SERVICES_DIRECTORY         = "META-INF/services/";

    private static final String                                      CANAL_DIRECTORY            = "META-INF/canal/";

    private static final String                                      DEFAULT_CLASSLOADER_POLICY = "internal";

    private static final Pattern                                     NAME_SEPARATOR             = Pattern.compile("\\s*[,]+\\s*");

    // 扩展加载器，key是CanalMQProducer.class，value是加载器，目前只有一个 CanalMQProducer类型的加载器
    private static final ConcurrentMap<Class<?>, ExtensionLoader<?>> EXTENSION_LOADERS          = new ConcurrentHashMap<>();

    // 实际的 CanalMQProducer 对象缓存
    private static final ConcurrentMap<Class<?>, Object>             EXTENSION_INSTANCES        = new ConcurrentHashMap<>();

    private static final ConcurrentMap<String, Object>               EXTENSION_KEY_INSTANCE     = new ConcurrentHashMap<>();

    private final Class<?>                                           type;

    private final String                                             classLoaderPolicy;

    private final ConcurrentMap<Class<?>, String>                    cachedNames                = new ConcurrentHashMap<>();

    // 维护外部拓展, k: 名称(kafka,rocketmq,rabbitmq), v: 对应的CanalMQProducer的class
    private final Holder<Map<String, Class<?>>>                      cachedClasses              = new Holder<>();

    private final ConcurrentMap<String, Holder<Object>>              cachedInstances            = new ConcurrentHashMap<>();

    private String                                                   cachedDefaultName;

    private ConcurrentHashMap<String, IllegalStateException>         exceptions                 = new ConcurrentHashMap<>();

    private static <T> boolean withExtensionAnnotation(Class<T> type) {
        return type.isAnnotationPresent(SPI.class);
    }

    /**
     * 获取指定类型的扩展加载器
     * @param type 指定类型的class，目前只有 CanalMQProducer.class
     * @param <T>  指定类型，目前只有 CanalMQProducer
     * @return
     */
    public static <T> ExtensionLoader<T> getExtensionLoader(Class<T> type) {
        return getExtensionLoader(type, DEFAULT_CLASSLOADER_POLICY);
    }

    @SuppressWarnings("unchecked")
    public static <T> ExtensionLoader<T> getExtensionLoader(Class<T> type, String classLoaderPolicy) {
        // region 类型检查
        if (type == null) throw new IllegalArgumentException("Extension type == null");
        if (!type.isInterface()) {
            throw new IllegalArgumentException("Extension type(" + type + ") is not interface!");
        }
        if (!withExtensionAnnotation(type)) {
            throw new IllegalArgumentException("Extension type(" + type + ") is not extension, because WITHOUT @"
                                               + SPI.class.getSimpleName() + " Annotation!");
        }
        // endregion

        // region 获取加载器，就是new一下，放到map里，再取出来
        ExtensionLoader<T> loader = (ExtensionLoader<T>) EXTENSION_LOADERS.get(type);
        if (loader == null) {
            EXTENSION_LOADERS.putIfAbsent(type, new ExtensionLoader<T>(type, classLoaderPolicy));
            loader = (ExtensionLoader<T>) EXTENSION_LOADERS.get(type);
        }
        // endregion
        
        return loader;
    }

    public ExtensionLoader(Class<?> type){
        this.type = type;
        this.classLoaderPolicy = DEFAULT_CLASSLOADER_POLICY;
    }

    public ExtensionLoader(Class<?> type, String classLoaderPolicy){
        this.type = type;
        this.classLoaderPolicy = classLoaderPolicy;
    }

    /**
     * 返回指定名字的扩展
     *
     * @param name 指定名称，kafka、rocketmq、rabbitmq
     * @return
     */
    @SuppressWarnings("unchecked")
    public T getExtension(String name, String spiDir, String standbyDir) {
        if (name == null || name.length() == 0) throw new IllegalArgumentException("Extension name == null");
        if ("true".equals(name)) {
            return getDefaultExtension(spiDir, standbyDir);
        }
        Holder<Object> holder = cachedInstances.get(name);
        if (holder == null) {
            cachedInstances.putIfAbsent(name, new Holder<>());
            holder = cachedInstances.get(name);
        }
        Object instance = holder.get();
        if (instance == null) {
            synchronized (holder) {
                instance = holder.get();
                if (instance == null) {
                    instance = createExtension(name, spiDir, standbyDir);  // 创建拓展，即指定的CanalMQProducer
                    holder.set(instance);
                }
            }
        }
        return (T) instance;
    }

    @SuppressWarnings("unchecked")
    public T getExtension(String name, String key, String spiDir, String standbyDir) {
        if (name == null || name.length() == 0) throw new IllegalArgumentException("Extension name == null");
        if ("true".equals(name)) {
            return getDefaultExtension(spiDir, standbyDir);
        }
        String extKey = name + "-" + StringUtils.trimToEmpty(key);
        Holder<Object> holder = cachedInstances.get(extKey);
        if (holder == null) {
            cachedInstances.putIfAbsent(extKey, new Holder<>());
            holder = cachedInstances.get(extKey);
        }
        Object instance = holder.get();
        if (instance == null) {
            synchronized (holder) {
                instance = holder.get();
                if (instance == null) {
                    instance = createExtension(name, key, spiDir, standbyDir);
                    holder.set(instance);
                }
            }
        }
        return (T) instance;
    }

    /**
     * 返回缺省的扩展，如果没有设置则返回<code>null</code>
     */
    public T getDefaultExtension(String spiDir, String standbyDir) {
        getExtensionClasses(spiDir, standbyDir);
        if (null == cachedDefaultName || cachedDefaultName.length() == 0 || "true".equals(cachedDefaultName)) {
            return null;
        }
        return getExtension(cachedDefaultName, spiDir, standbyDir);
    }

    @SuppressWarnings("unchecked")
    public T createExtension(String name, String spiDir, String standbyDir) {
        // region 获取指定名称的拓展的 CanalMQProducer.class
        Class<?> clazz = getExtensionClasses(spiDir, standbyDir).get(name);
        if (clazz == null) {
            throw new IllegalStateException("Extension instance(name: " + name + ", class: " + type
                                            + ")  could not be instantiated: class could not be found");
        }
        // endregion

        // region 实例化并返回 CanalMQProducer
        try {
            T instance = (T) EXTENSION_INSTANCES.get(clazz);
            if (instance == null) {
                EXTENSION_INSTANCES.putIfAbsent(clazz, (T) clazz.newInstance());  // 实例化并存如本地缓存
                instance = (T) EXTENSION_INSTANCES.get(clazz);
            }
            return instance;
        } catch (Throwable t) {
            throw new IllegalStateException("Extension instance(name: " + name + ", class: " + type
                                            + ")  could not be instantiated: " + t.getMessage(), t);
        }
        // endregion
    }

    @SuppressWarnings("unchecked")
    private T createExtension(String name, String key, String spiDir, String standbyDir) {
        Class<?> clazz = getExtensionClasses(spiDir, standbyDir).get(name);
        if (clazz == null) {
            throw new IllegalStateException("Extension instance(name: " + name + ", class: " + type
                                            + ")  could not be instantiated: class could not be found");
        }
        try {
            T instance = (T) EXTENSION_KEY_INSTANCE.get(name + "-" + key);
            if (instance == null) {
                EXTENSION_KEY_INSTANCE.putIfAbsent(name + "-" + key, clazz.newInstance());
                instance = (T) EXTENSION_KEY_INSTANCE.get(name + "-" + key);
            }
            return instance;
        } catch (Throwable t) {
            throw new IllegalStateException("Extension instance(name: " + name + ", class: " + type
                                            + ")  could not be instantiated: " + t.getMessage(), t);
        }
    }

    /**
     * 把 {canal_dir}/plugin 下的jar包加载进来，将对应的Producer存入 {@link cachedClasses} 中
     * @param spiDir
     * @param standbyDir
     * @return
     */
    private Map<String, Class<?>> getExtensionClasses(String spiDir, String standbyDir) {
        Map<String, Class<?>> classes = cachedClasses.get();
        if (classes == null) {
            synchronized (cachedClasses) {
                classes = cachedClasses.get();
                if (classes == null) {
                    classes = loadExtensionClasses(spiDir, standbyDir);  // 加载目录下的所有拓展
                    cachedClasses.set(classes);  // 将加载到的拓展全部设置本地缓存中
                }
            }
        }

        return classes;
    }

    private String getJarDirectoryPath() {
        URL url = Thread.currentThread().getContextClassLoader().getResource("");
        String dirtyPath;
        if (url != null) {
            dirtyPath = url.toString();
        } else {
            File file = new File("");
            dirtyPath = file.getAbsolutePath();
        }
        String jarPath = dirtyPath.replaceAll("^.*file:/", ""); // removes
                                                                // file:/ and
                                                                // everything
                                                                // before it
        jarPath = jarPath.replaceAll("jar!.*", "jar"); // removes everything
                                                       // after .jar, if .jar
                                                       // exists in dirtyPath
        jarPath = jarPath.replaceAll("%20", " "); // necessary if path has
                                                  // spaces within
        if (!jarPath.endsWith(".jar")) { // this is needed if you plan to run
                                         // the app using Spring Tools Suit play
                                         // button.
            jarPath = jarPath.replaceAll("/classes/.*", "/classes/");
        }
        Path path = Paths.get(jarPath).getParent(); // Paths - from java 8
        if (path != null) {
            return path.toString();
        }
        return null;
    }

    /**
     * 加载所有的外部拓展 CanalMQProducer
     * @param spiDir
     * @param standbyDir
     * @return 返回Map，存放所有的 名称-Producer，如：rabbitmq-CanalRabbitMQProducer
     */
    private Map<String, Class<?>> loadExtensionClasses(String spiDir, String standbyDir) {
        // region 设置默认扩展名称（kafka）
        final SPI defaultAnnotation = type.getAnnotation(SPI.class);
        if (defaultAnnotation != null) {
            String value = defaultAnnotation.value();
            if ((value = value.trim()).length() > 0) {
                String[] names = NAME_SEPARATOR.split(value);
                if (names.length > 1) {
                    throw new IllegalStateException("more than 1 default extension name on extension " + type.getName()
                                                    + ": " + Arrays.toString(names));
                }
                if (names.length == 1) cachedDefaultName = names[0];
            }
        }
        // endregion

        Map<String, Class<?>> extensionClasses = new HashMap<>(); //返回值

        if (spiDir != null && standbyDir != null) {
            // 1. plugin folder，customized extension classLoader
            // （jar_dir/plugin）
            String dir = File.separator + this.getJarDirectoryPath() + spiDir; // +
                                                                               // "plugin";

            File externalLibDir = new File(dir);
            if (!externalLibDir.exists()) {
                externalLibDir = new File(File.separator + this.getJarDirectoryPath() + standbyDir);
            }
            logger.info("extension classpath dir: " + externalLibDir.getAbsolutePath());
            if (externalLibDir.exists()) {
                // 循环plugin下的所有jar包，加载 META-INF/canal/ 文件指定的名称和类名，放到返回值map里
                File[] files = externalLibDir.listFiles((dir1, name) -> name.endsWith(".jar"));
                if (files != null) {
                    for (File f : files) {
                        URL url;
                        try {
                            url = f.toURI().toURL();
                        } catch (MalformedURLException e) {
                            throw new RuntimeException("load extension jar failed!", e);
                        }

                        ClassLoader parent = Thread.currentThread().getContextClassLoader();
                        URLClassLoader localClassLoader;
                        if (classLoaderPolicy == null || "".equals(classLoaderPolicy)
                            || DEFAULT_CLASSLOADER_POLICY.equalsIgnoreCase(classLoaderPolicy)) {
                            localClassLoader = new URLClassExtensionLoader(new URL[] { url });
                        } else {
                            localClassLoader = new URLClassLoader(new URL[] { url }, parent);
                        }

                        loadFile(extensionClasses, CANAL_DIRECTORY, localClassLoader);
                        loadFile(extensionClasses, SERVICES_DIRECTORY, localClassLoader);
                    }
                }
            }
        }

        // 2. load inner extension class with default classLoader
        ClassLoader classLoader = findClassLoader();
        loadFile(extensionClasses, CANAL_DIRECTORY, classLoader);
        loadFile(extensionClasses, SERVICES_DIRECTORY, classLoader);

        return extensionClasses;
    }

    private void loadFile(Map<String, Class<?>> extensionClasses, String dir, ClassLoader classLoader) {
        String fileName = dir + type.getName();
        try {
            Enumeration<URL> urls;
            if (classLoader != null) {
                urls = classLoader.getResources(fileName);
            } else {
                urls = ClassLoader.getSystemResources(fileName);
            }
            if (urls != null) {
                while (urls.hasMoreElements()) {
                    URL url = urls.nextElement();
                    try {
                        BufferedReader reader = null;
                        try {
                            reader = new BufferedReader(new InputStreamReader(url.openStream(), StandardCharsets.UTF_8));
                            String line = null;
                            while ((line = reader.readLine()) != null) {
                                final int ci = line.indexOf('#');
                                if (ci >= 0) line = line.substring(0, ci);
                                line = line.trim();
                                if (line.length() > 0) {
                                    try {
                                        String name = null;
                                        int i = line.indexOf('=');
                                        if (i > 0) {
                                            name = line.substring(0, i).trim();
                                            line = line.substring(i + 1).trim();
                                        }
                                        if (line.length() > 0) {
                                            Class<?> clazz = classLoader.loadClass(line);
                                            // Class<?> clazz =
                                            // Class.forName(line, true,
                                            // classLoader);
                                            if (!type.isAssignableFrom(clazz)) {
                                                throw new IllegalStateException("Error when load extension class(interface: "
                                                                                + type
                                                                                + ", class line: "
                                                                                + clazz.getName()
                                                                                + "), class "
                                                                                + clazz.getName()
                                                                                + "is not subtype of interface.");
                                            } else {
                                                try {
                                                    clazz.getConstructor(type);
                                                } catch (NoSuchMethodException e) {
                                                    clazz.getConstructor();
                                                    String[] names = NAME_SEPARATOR.split(name);
                                                    if (names != null && names.length > 0) {
                                                        for (String n : names) {
                                                            if (!cachedNames.containsKey(clazz)) {
                                                                cachedNames.put(clazz, n);
                                                            }
                                                            Class<?> c = extensionClasses.get(n);
                                                            if (c == null) {
                                                                extensionClasses.put(n, clazz);
                                                            } else if (c != clazz) {
                                                                cachedNames.remove(clazz);
                                                                throw new IllegalStateException("Duplicate extension "
                                                                                                + type.getName()
                                                                                                + " name " + n + " on "
                                                                                                + c.getName() + " and "
                                                                                                + clazz.getName());
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    } catch (Throwable t) {
                                        IllegalStateException e = new IllegalStateException("Failed to load extension class(interface: "
                                                                                            + type
                                                                                            + ", class line: "
                                                                                            + line
                                                                                            + ") in "
                                                                                            + url
                                                                                            + ", cause: "
                                                                                            + t.getMessage(),
                                            t);
                                        exceptions.put(line, e);
                                    }
                                }
                            } // end of while read lines
                        } finally {
                            if (reader != null) {
                                reader.close();
                            }
                        }
                    } catch (Throwable t) {
                        logger.error("Exception when load extension class(interface: " + type + ", class file: " + url
                                     + ") in " + url, t);
                    }
                } // end of while urls
            }
        } catch (Throwable t) {
            logger.error("Exception when load extension class(interface: " + type + ", description file: " + fileName
                         + ").", t);
        }
    }

    private static ClassLoader findClassLoader() {
        return ExtensionLoader.class.getClassLoader();
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "[" + type.getName() + "]";
    }

    private static class Holder<T> {

        private volatile T value;

        private void set(T value) {
            this.value = value;
        }

        private T get() {
            return value;
        }

    }
}
