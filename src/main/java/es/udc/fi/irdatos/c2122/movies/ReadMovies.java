package es.udc.fi.irdatos.c2122.movies;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import es.udc.fi.irdatos.c2122.util.ObjectReaderUtils;

public class ReadMovies {

    /*
    * This does not work with a packaged application, only when executing in
    * the IDE or with mvn exec:java
    */
    private static final Path DEFAULT_COLLECTION_PATH = Paths.get("src", "main", "resources");

    private static String MOVIE_METADATA_FILE_NAME = "movies.csv";

    private static final ObjectReader SCRIPT_READER = JsonMapper.builder().findAndAddModules().build()
            .readerFor(MovieScript.class);

    /**
     * Utility method to read a script text from a file to a String
     *
     * @param scriptPath path of the script file
     * @return a String with the text of the script
     */
    private static final String readScript(Path scriptPath) {
        MovieScript script;
        try {
            script = SCRIPT_READER.readValue(scriptPath.toFile());
        } catch (IOException e) {
            System.err.println("Error reading script file: " + scriptPath);
            e.printStackTrace();
            return "";
        }

        StringBuilder sb = new StringBuilder();
        sb.append(script.title());
        sb.append('\n');
        for (MovieScript.Scene scene : script.scenes()) {
            sb.append(scene.transition());
            sb.append('\n');
            sb.append(scene.header());
            sb.append('\n');
            for (MovieScript.SceneContent content : scene.contents()) {
                sb.append(content.text());
                sb.append('\n');
            }
        }

        return sb.toString();
    }

    public static void main(String[] args) {
        Path collectionPath;
        if (args.length > 0) {
            collectionPath = Paths.get(args[0]);
        } else {
            collectionPath = DEFAULT_COLLECTION_PATH;
        }

        Path moviesPath = collectionPath.resolve(MOVIE_METADATA_FILE_NAME);

        /*
        * Define the schema of the csv file. The schema defines the columns and some other information
        * about the file.
        *   - emptySchema(): start from an empty schema
        *   - withHeader(): field names are declared in the header (we don't define the columns,
        *                   we let the library read the column names from the file)
        *   - withArrayElementSeparator("; "): fields that contain multiple values have the values
        *                                      separated by the string "; "
        */
        CsvSchema schema = CsvSchema.emptySchema().withHeader().withArrayElementSeparator("; ");

        /*
        * Create an ObjectReader, an object that is used to read values of the
        * given type from the csv file. The file is read using the provided schema
        */
        ObjectReader reader = new CsvMapper().readerFor(Movie.class).with(schema);

        List<Movie> movies;
        try {
            movies = ObjectReaderUtils.readAllValues(moviesPath, reader);
        } catch (IOException ex) {
            System.err.println("Error when trying to read and parse the input file");
            ex.printStackTrace();
            return;
        }

        for (Movie movie : movies) {
            System.out.println(movie);
            String scriptFilename = movie.script();
            if (scriptFilename != null && !scriptFilename.equals("")) {
                String movieScript = readScript(collectionPath.resolve(scriptFilename));
                System.out.println(movieScript);
                System.out.println();
            }
        }

    }
}
