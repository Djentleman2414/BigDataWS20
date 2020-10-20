package multiin;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.Optional;

public class MapperUtils {

    public static Path getPath(InputSplit split) {
        return (Path) getFileSplit(split).map(FileSplit::getPath).orElseThrow(() -> 
            new AssertionError("cannot find path from split " + split.getClass()));
    }

    public static Optional<FileSplit> getFileSplit(InputSplit split) {
        if (split instanceof FileSplit) {
            return Optional.of((FileSplit)split);
        } else if (TaggedInputSplit.clazz.isInstance(split)) {
            return getFileSplit(TaggedInputSplit.getInputSplit(split));
        } else {
            return Optional.empty();
        }
    }

    private static final class TaggedInputSplit {
        private static final Class<?> clazz;
        private static final MethodHandle method;

        static {
            try {
                clazz = Class.forName("org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit");
                Method m = clazz.getDeclaredMethod("getInputSplit");
                m.setAccessible(true);
                method = MethodHandles.lookup().unreflect(m).asType(
                    MethodType.methodType(InputSplit.class, InputSplit.class));
            } catch (ReflectiveOperationException e) {
                throw new AssertionError(e);
            }
        }

        static InputSplit getInputSplit(InputSplit o) {
            try {
                return (InputSplit) method.invokeExact(o);
            } catch (Throwable e) {
                throw new AssertionError(e);
            }
        }
    }

    private MapperUtils() { }

}

