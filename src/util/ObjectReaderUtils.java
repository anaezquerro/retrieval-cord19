package es.udc.fi.irdatos.c2122.util;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectReader;

/**
 * Class with utility methods for Jackson {@link ObjectReader} objects
 */
public class ObjectReaderUtils {

  private ObjectReaderUtils() {}

  /**
   * Convenience method to read all values of a file to a {@link List}. Using this
   * methods addresses the shortcomings of Java's type inference when chaining
   * method calls and produces a {@link List} ot the target type T instead of a
   * List of Object
   *
   * @param <T>    The type of the objects to read (and the elements of the
   *               resulting List)
   * @param path   The path of the input file
   * @param reader The object reader
   * @return a list of all the objects of type T parsed from the file
   * @throws IOException exceptions are propagated
   */
  public static <T> List<T> readAllValues(Path path, ObjectReader reader) throws IOException {
    return reader.<T>readValues(path.toFile()).readAll();
  }
}
