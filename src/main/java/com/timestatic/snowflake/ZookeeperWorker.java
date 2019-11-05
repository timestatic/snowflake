package com.timestatic.snowflake;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author: timestatic
 * @Date: 2019/11/4 9:44
 */
public final class ZookeeperWorker {
    private final static Logger LOGGER = LoggerFactory.getLogger(ZookeeperWorker.class);

    private static final Long MAX_NODE_NUM = 1024L;
    private static final String PARENT_FOREVER_PATH = "/leaf-snowflake";
    private static final String APP_PATH = "/appName";
    private static final String PARENT_PATH = PARENT_FOREVER_PATH + APP_PATH;
    /**
     * 临时节点
     */
    private static final String TEMP_CHILD_PATH = "/instanceId";
    /**
     * path: /leaf-snowflake/appName/instanceId
     */
    private static final String COMPLETE_PATH = PARENT_FOREVER_PATH + APP_PATH + TEMP_CHILD_PATH;

    private static ZkClient zkClient;
    private static String zkAddress;

    private volatile static boolean initialized = false;

    private static volatile Long workId;

    public static Long getWorkId() {
        return workId;
    }

    public synchronized static void init(String zookeeperAddress) {
        if (initialized) {
            return;
        }
        initialized = true;

        zkAddress = zookeeperAddress;
        createByZookeeper();
        addWatch(zkClient);
    }

    private static void createByZookeeper() {
        String address = zkAddress;
        zkClient = new ZkClient(address);

        List<String> children;
        try {
            children = zkClient.getChildren(PARENT_PATH);
            if (children.isEmpty()) {
                zkClient.delete(PARENT_PATH);
                zkClient.createPersistent(PARENT_PATH, true);
            }
        } catch (ZkNoNodeException e) {
            zkClient.createPersistent(PARENT_PATH, true);
        }

        String data = String.valueOf(System.currentTimeMillis());

        // 创建临时顺序节点, 获取id
        String resultPath;
        try {
            resultPath = zkClient.createEphemeralSequential(COMPLETE_PATH, data);
        } catch (ZkNoNodeException e) {
            zkClient.createPersistent(PARENT_PATH, true);
            resultPath = zkClient.createEphemeralSequential(COMPLETE_PATH, data);
        }

        Long id = getIdFromPath(resultPath);
        if (id < MAX_NODE_NUM) {
            createWork(id);
            return;
        }

        zkClient.delete(resultPath);

        children = zkClient.getChildren(PARENT_PATH);
        Set<Long> ids = children.stream().map(item -> getIdFromChildPath(item))
                .filter(item -> item != null).collect(Collectors.toSet());
        for (Long i = 0L; i < MAX_NODE_NUM; i++) {
            if (!ids.contains(i)) {
                try {
                    String path = COMPLETE_PATH + String.format("%010d", i);
                    zkClient.createEphemeral(path, data);
                    createWork(id);
                    return;
                } catch (Exception e) {
                    continue;
                }
            }
        }

    }

    private static void addWatch(ZkClient client) {
        // watch app节点
        client.subscribeDataChanges(PARENT_PATH, new IZkDataListener() {
            @Override
            public void handleDataChange(String dataPath, Object data) throws Exception {
            }

            @Override
            public void handleDataDeleted(String dataPath) throws Exception {
                LOGGER.warn("[snowflake] path={} data deleted, retry init", dataPath);
                createByZookeeper();
            }
        });

        // watch连接
        client.subscribeStateChanges(new IZkStateListener() {
            @Override
            public void handleStateChanged(Watcher.Event.KeeperState state) throws Exception {
                if (state == Watcher.Event.KeeperState.Disconnected) {
                    LOGGER.warn("[snowflake] zkclient disconnect with server ....");
                }
            }

            @Override
            public void handleNewSession() throws Exception {
                LOGGER.warn("[snowflake] handleNewSession retry init");
                createByZookeeper();
            }
        });
    }

    private static Long getIdFromChildPath(String path) {
        try {
            String idStr = path.replace("instanceId", "");
            return Long.valueOf(idStr);
        } catch (Exception e) {
            return null;
        }
    }

    private static Long getIdFromPath(String path) {
        String idStr = path.replace(COMPLETE_PATH, "");
        return Long.valueOf(idStr);
    }

    private static void createWork(Long id) {
        workId = id;
        SnowFlakeWorker.initSnowFlakeWorker(id);
    }

}
