package com.kylinolap.common.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Ignore;

/**
 * Created by honma on 6/6/14.
 */
public class InstallJarIntoMavenTest {

    @Ignore("convenient trial tool for dev")
    public void testInstall() throws IOException {
        File folder = new File("/export/home/b_kylin/tmp");
        File out = new File("/export/home/b_kylin/tmp/out.sh");
        out.createNewFile();
        FileWriter fw = new FileWriter(out);

        for (File file : folder.listFiles()) {
            String name = file.getName();

            if (!name.endsWith(".jar"))
                continue;

            int firstSlash = name.indexOf('-');
            int lastDot = name.lastIndexOf('.');
            String groupId = name.substring(0, firstSlash);

            Pattern pattern = Pattern.compile("-\\d");
            Matcher match = pattern.matcher(name);
            match.find();
            String artifactId = name.substring(0, match.start());
            String version = name.substring(match.start() + 1, lastDot);

            fw.write(String.format("mvn install:install-file -Dfile=%s -DgroupId=%s -DartifactId=%s -Dversion=%s -Dpackaging=jar", name, "org.apache." + groupId, artifactId, version));
            fw.write("\n");
        }
        fw.close();
    }

}
