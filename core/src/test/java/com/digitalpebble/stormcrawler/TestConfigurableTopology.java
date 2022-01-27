package com.digitalpebble.stormcrawler;

import java.util.List;
import org.apache.storm.streams.Pair;
import org.junit.Assert;
import org.junit.Test;

public class TestConfigurableTopology {

    @Test
    public void parse_args_without_conf() {
        String[] argsIn = new String[] {"-foo", "bar", "-foobar"};
        final Pair<List<String>, String[]> stringPair =
                ConfigurableTopology.extractConfFromArgs(argsIn);

        Assert.assertNull(stringPair.value1);
        Assert.assertNotNull(stringPair.value2);

        String[] argsExpect = new String[] {"-foo", "bar", "-foobar"};
        Assert.assertArrayEquals(argsExpect, stringPair.value2);
    }

    @Test
    public void parse_args_with_single_conf() {
        String[] argsIn = new String[] {"-foo", "bar", "-conf", "crawler-default.yaml", "-foobar"};
        final Pair<List<String>, String[]> stringPair =
                ConfigurableTopology.extractConfFromArgs(argsIn);

        Assert.assertNotNull(stringPair.value1);
        Assert.assertNotNull(stringPair.value2);

        Assert.assertSame(1, stringPair.value1.size());

        Assert.assertEquals("crawler-default.yaml", stringPair.value1.get(0));

        String[] argsExpect = new String[] {"-foo", "bar", "-foobar"};
        Assert.assertArrayEquals(argsExpect, stringPair.value2);
    }

    @Test
    public void parse_args_with_multiple_conf() {
        String[] argsIn =
                new String[] {
                    "-conf",
                    "crawler-default1.yaml",
                    "-foo",
                    "bar",
                    "-conf",
                    "crawler-default2.yaml",
                    "-foobar",
                    "-conf",
                    "crawler-default3.yaml"
                };
        final Pair<List<String>, String[]> stringPair =
                ConfigurableTopology.extractConfFromArgs(argsIn);

        Assert.assertNotNull(stringPair.value1);
        Assert.assertNotNull(stringPair.value2);

        Assert.assertSame(3, stringPair.value1.size());

        Assert.assertEquals("crawler-default1.yaml", stringPair.value1.get(0));
        Assert.assertEquals("crawler-default2.yaml", stringPair.value1.get(1));
        Assert.assertEquals("crawler-default3.yaml", stringPair.value1.get(2));

        String[] argsExpect = new String[] {"-foo", "bar", "-foobar"};
        Assert.assertArrayEquals(argsExpect, stringPair.value2);
    }

    @Test
    public void parse_args_with_invalid_conf_and_fail() {
        String[] argsIn = new String[] {"-foo", "bar", "-foobar", "-conf"};
        //noinspection ResultOfMethodCallIgnored
        Assert.assertThrows(
                "A conf without value can not be processed!",
                ConfigurableTopology.RequiredArgumentValueMissingException.class,
                () -> ConfigurableTopology.extractConfFromArgs(argsIn));
    }
}
