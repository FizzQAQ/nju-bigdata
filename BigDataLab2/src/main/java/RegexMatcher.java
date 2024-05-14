
import java.util.regex.Pattern;

public class RegexMatcher {
    static Pattern pattern = Pattern.compile("\\w+(-\\w+)*('\\w+)?", Pattern.CASE_INSENSITIVE);
}
