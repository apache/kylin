package org.apache.kylin.dict.lookup;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.TableDesc;

public class SnapshotCLI {

    public static void main(String[] args) throws IOException {
        if ("rebuild".equals(args[0]))
            rebuild(args[1], args[2]);
    }

    private static void rebuild(String table, String overwriteUUID) throws IOException {
        KylinConfig conf = KylinConfig.getInstanceFromEnv();
        MetadataManager metaMgr = MetadataManager.getInstance(conf);
        SnapshotManager snapshotMgr = SnapshotManager.getInstance(conf);
        
        TableDesc tableDesc = metaMgr.getTableDesc(table);
        if (tableDesc == null)
            throw new IllegalArgumentException("Not table found by " + table);
        
        SnapshotTable snapshot = snapshotMgr.rebuildSnapshot(new HiveTable(metaMgr, table), tableDesc, overwriteUUID);
        System.out.println("resource path updated: " + snapshot.getResourcePath());
    }
}
