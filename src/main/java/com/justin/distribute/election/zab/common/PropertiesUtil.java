package com.justin.distribute.election.zab.common;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class PropertiesUtil {

    private PropertiesUtil() {
    }

    public static PropertiesUtil getInstance() {
        return new PropertiesUtil();
    }

    public static Properties getProtParams() {
        return PropertiesUtil.getInstance().getPortParams();
    }

    public static Map<Integer, String> getNodesAddress() {
        Map<Integer, String> nodesAddress = new HashMap<>();
        for (int i = 1; ; i++) {
            String address = getClusterNodesAddress(i);
            if (address == null || address.equals("")) {
                break;
            }
            nodesAddress.put(i, address);
        }
        return nodesAddress;
    }

    public static String getClusterNodesAddress(final int id) {
        return getProtParams().getProperty("node." + id);
    }

    public static Integer getNodeId() {
        return Integer.parseInt(System.getProperty("nodeId"));
    }

    public static String getLogDir() {
        return getDataDir() + "log_" + getNodeId();
    }

    public static String getCommittedDir() {
        return getDataDir() + "committed_" + getNodeId();
    }

    public static String getDataDir() {
        return getProtParams().getProperty("node.data.dir");
    }

    private Properties getPortParams() {
        String configPath = System.getProperty("config");
        File f = new File(configPath);
        InputStream is;
        try {
            is = new FileInputStream(f);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        Properties prop = new Properties();
        try {
            prop.load(is);
        } catch (IOException e1) {
            e1.printStackTrace();
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return prop;
    }
}
