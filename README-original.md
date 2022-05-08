# GCED - Recuperación de Información - Assignment

Template for the assignment of the Information Retrieval course - GCED - UDC

The template includes code examples for reading CSV files and JSON files

## Java Records

Records are a special kind of classes introduced in Java 14. They allow to define data aggregates (classes whose whole purpose is to only hold data) with much less code. Records classes are **immutable**, define sensible implementations for the `equals`, `hashCode` and `toString` methods, and a constructor with the same signature as the record header. A record declaration like:

```Java
record Rectangle(double length, double width) {}
```

Gives as a result a class similar to:

```Java
class Rectangle {
  private final double length;
  private final double width;

  public Rectangle(double length, double width) {
    this.length = length;
    this.width = width;
  }

  public double length() { return length; }
  public double width() { return width; }

  /* Implementation of equals() compares two record field by field instead of the
   * default comparison by object identity of the implementation in the base
   * Object class. implementation of hashCode() is sensible to this change
   */
  public boolean equals(Object other) { ... }
  public int hashCode() { ... }

  /* Implementation of toString() returns a representation that includes the
   * record name and the values of all the fields
   */
  public String toString() { ... }
}
```

More information: [Record Classes](https://docs.oracle.com/en/java/javase/17/language/records.html)

The example code makes use of this type of classes for easier definition of data classes to read the files

## Parsing CSV files

To parse CSV files the example code uses the [Jackson data format text][1] module for reading CSV files.

Each row in the CSV file is read into a record [`Movies`](src/main/java/es/udc/fi/irdatos/c2122/movies/Movie.java) with the fields we are interested from the CSV. The record definition uses some Java annotations defined by Jackson to instruct the library how to map the fields from the file to the attributes of the class. These annotations are:
  - `@JsonIgnoreProperties(ignoreUnknown = true)`: this annotation tells Jackson to ignore fields in the source file not defined in the class. This contrasts with the default behavior of failing to parse the file in that case. In the example CSV there is a column `rating` that has no corresponding field in the `Movie` record. Without this annotation the parsing of the movies file would fail. This annotation applies to the whole class.
  - `@JsonProperty("imdb_id")`: this annotation tells Jackson to map the field in the source file to the attribute that is annotated. In the sample code the parser will map the contents of the `imdb_id` column to the attribute `imdbId` in the record. This is useful when we want to keep some naming convention in the code that would be broken by the names of the fields in the file, such as this case. The use of this annotation is mandatory if the name of the field is not a valid identifier in Java (e.g. the name of the field is a reserved word in the language)

To parse a file we need to take a couple of steps:
  - First we need to define the scheme of the file, i.e. the columns it contains and the order in which they appear. In the case of the example CSV, the file has a header row. We can instruct Jackson to use the first row as the schema for the file. We also tell Jackon to use the string `"; "` as the separator of the values of multi-valued fields (in the case of the example, the `cast` field). With this we can parse the values to a List object or an array (we get a `List<String>`  for the cast):
  ```Java
  CsvSchema schema = CsvSchema.emptySchema().withHeader().withArrayElementSeparator("; ");
  ```
  - With the schema defined we need to create a reader object, that will do the actual parsing. We need to indicate the target class for the data and the schema of the source file:
  ```Java
  ObjectReader reader = new CsvMapper().readerFor(Movie.class).with(schema);
  ```
  - The reader object provides several methods for reading the data. The template includes a utility method in the `ObjectReaderUtils` class for reading all the rows from a file to a List. This method can be used for the assignment:
  ```Java
  // The actual code in the example manages possible exceptions during the process
  List<Movie> movies = ObjectReaderUtils.readAllValues(moviesPath, reader);
  ```

More information about this Jackson module, its usage and links to the javadoc can be found at the [GitHub page][1] of the project.

[1]: https://github.com/FasterXML/jackson-dataformats-text/tree/master/csv


## Parsing JSON files

Similar to the case of CSV files, to parse JSON files the example uses the [Jackson databind][2] library.

In this particular case the file to be read contains a single object (a movie script in this case). The structure of the data in the JSON includes nesting, with some fields of the root object including other objects. To represent this structure in Java several classes are needed. In particular the target for reading the root object is defined in the `MovieScript` record. This class has an attribute for a List of objects of type `Scene`. This corresponds to the structure of the source file, where the field `scenes` contains a JavaScript array of JavaScript objects. Each of this objects is mapped to the `Scene` class. Moreover, the objects for the scenes contain a field, `contents`, with an array of objects. This is mapped to a `List<SceneContent>`, where `SceneContent` is a third record with the structure of this data.

The class/record definitions that will hold the data once read can use the same annotation as in the CSV case: `@JsonIgnoreProperties` and `@JsonProperty`. The use cases of these annotations are the same.

Once we have defined the structure of the source file with our class/record definition(s) we can parse the contents of a file using a reader, created in similar fashion as when parsing CSV, but with the apropiarte class. In this case we don't need to define the schame of the file, the class structure is enough for the library. We need to create a reader object:

```Java
// In the sample code the object is stored in a static attribute so it can be reused
ObjectReader reader = JsonMapper.builder().findAndAddModules().build().readerFor(MovieScript.class);
```

This reader object is of the same type as the one in the CSV case, so it has the same methods. But they are different object, that have been created differently to read different data. As the file in the example contains a single object we can use the `readValue` method to read the file:

```Java
// Again, the code in the example handles exceptions in the process
MovieScript script = reader.readValue(scriptPath.toFile());
```

More information on the library, including links to the documentation, can be found at the [GitHub repository][2] page for the library.

[2]: https://github.com/FasterXML/jackson-databind
