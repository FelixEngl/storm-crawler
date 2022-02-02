package com.digitalpebble.stormcrawler.protocol;

import static org.junit.Assert.fail;

import com.digitalpebble.stormcrawler.protocol.selenium.RemoteDriverProtocol;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import java.io.FileNotFoundException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import org.apache.storm.Config;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class RemoteDriverProtocolTest {

    @Parameterized.Parameter public String pathToConfig;

    @Parameterized.Parameter(1)
    public Object[] expected;

    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<Object> parametersForTesting() {
        try {

            return Arrays.asList(
                    new Object[][] {
                        {
                            "selenium/selenium_mixed_config.yaml",
                            new URL[] {
                                new URL("http://stormcrawler.net/"),
                                new URL("http://stormcrawler.net/"),
                                new URL("http://stormcrawler.net/"),
                                new URL("http://127.0.0.1:4444/"),
                                new URL("http://127.0.0.1:4444/"),
                                new URL("http://127.0.0.1:4444/"),
                                new URL("http://view-localhost:4444"),
                                new URL("http://localhost:4444/"),
                            }
                        },
                        {
                            "selenium/selenium_single_simple_config.yaml",
                            new URL[] {
                                new URL("http://stormcrawler.net/"),
                            }
                        },
                        {
                            "selenium/selenium_multiple_simple_config.yaml",
                            new URL[] {
                                new URL("http://stormcrawler.net/"),
                                new URL("https://google.de/"),
                                new URL("https://amazon.de/"),
                            }
                        },
                        {
                            "selenium/selenium_single_complex_config.yaml",
                            new URL[] {
                                new URL("http://127.0.0.1:9000/"),
                            }
                        },
                        {
                            "selenium/selenium_multiple_complex_config.yaml",
                            new URL[] {
                                new URL("http://127.0.0.1:9000/"),
                                new URL("http://stormcrawler.net/"),
                                new URL("http://127.0.0.1:5555/"),
                            }
                        },
                    });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void test_configs() {
        Config config = new Config();
        try {
            ConfUtils.loadConfigIntoTarget(pathToConfig, config);
        } catch (FileNotFoundException e) {
            fail("File was not found!");
            throw new RuntimeException(e);
        }
        List<URL> urls;
        try {
            urls = RemoteDriverProtocol.loadURLsFromConfig(config);
        } catch (MalformedURLException e) {
            fail("There shouldn't be any malformed urls.");
            throw new RuntimeException(e);
        }

        Assert.assertNotNull(urls);

        Assert.assertArrayEquals(expected, urls.toArray());
    }
}
