package com.kylinolap.cube.model;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.persistence.JsonSerializer;
import com.kylinolap.common.persistence.ResourceStore;
import com.kylinolap.common.util.JsonUtil;
import com.kylinolap.cube.CubeDescManager;
import com.kylinolap.cube.CubeDescUpgrader;
import com.kylinolap.metadata.MetadataConstances;
import com.kylinolap.metadata.MetadataManager;
import com.kylinolap.metadata.model.TableDesc;
import com.kylinolap.metadata.project.ProjectInstance;
import com.kylinolap.metadata.project.ProjectManager;
import com.kylinolap.metadata.project.RealizationEntry;
import com.kylinolap.metadata.realization.RealizationType;

/**
 * This is the utility class to migrate the Kylin metadata format from v1 to v2;
 * 
 * @author shaoshi
 *
 */
public class CubeMetadataUpgrade {

    private KylinConfig config = null;
    private ResourceStore store;

    private static final Log logger = LogFactory.getLog(CubeMetadataUpgrade.class);

    public CubeMetadataUpgrade() {
        config = KylinConfig.getInstanceFromEnv();
        store = getStore();
    }
    private void upgradeCubeDesc() throws IOException {
        logger.info("Reloading Cube Metadata from folder " + store.getReadableResourcePath(ResourceStore.CUBE_DESC_RESOURCE_ROOT));

        List<String> paths = store.collectResourceRecursively(ResourceStore.CUBE_DESC_RESOURCE_ROOT, MetadataConstances.FILE_SURFIX);
        for (String path : paths) {
            CubeDesc ndesc;
            try {
                ndesc = store.getResource(path, CubeDesc.class, CubeDescManager.CUBE_DESC_SERIALIZER);
            } catch (IOException e) {
                logger.debug("Get exception when load CubeDesc on " + path + ", going to do CubeDesc upgrade...");
                CubeDescUpgrader upgrade = new CubeDescUpgrader(path);
                ndesc = upgrade.upgrade();
                ndesc.setUpgraded(true);
                logger.debug("CubeDesc upgrade successful for " + path);
            }
            if (path.equals(ndesc.getResourcePath()) == false) {
                logger.error("Skip suspicious desc at " + path + ", " + ndesc + " should be at " + ndesc.getResourcePath());
                continue;
            }

            if (ndesc.isUpgraded()) {
                getStore().putResource(ndesc.getModel().getResourcePath(), ndesc.getModel(), MetadataManager.MODELDESC_SERIALIZER);
                getStore().putResource(ndesc.getResourcePath(), ndesc, CubeDescManager.CUBE_DESC_SERIALIZER);
            }
        }

    }

    private void upgradeTableDesc() throws IOException {
        logger.debug("Reloading SourceTable from folder " + store.getReadableResourcePath(ResourceStore.TABLE_RESOURCE_ROOT));

        List<String> paths = store.collectResourceRecursively(ResourceStore.TABLE_RESOURCE_ROOT, MetadataConstances.FILE_SURFIX);
        for (String path : paths) {
            TableDesc t = store.getResource(path, TableDesc.class, MetadataManager.TABLE_SERIALIZER);
            t.init();

            // if it only has 1 "." in the path, delete the old resource if it exists
            if (path.substring(path.indexOf(".")).length() == MetadataConstances.FILE_SURFIX.length()) {
                String old_path = t.getResourcePathV1();
                if (getStore().exists(old_path)) {
                    getStore().deleteResource(old_path);
                    // the new source will be new;
                    t.setLastModified(0);
                    getStore().putResource(t.getResourcePath(), t, MetadataManager.TABLE_SERIALIZER);
                }
            }
        }

    }

    private void upgradeTableDesceExd() throws IOException {
        logger.debug("Reloading SourceTable exd info from folder " + store.getReadableResourcePath(ResourceStore.TABLE_EXD_RESOURCE_ROOT));

        List<String> paths = store.collectResourceRecursively(ResourceStore.TABLE_EXD_RESOURCE_ROOT, MetadataConstances.FILE_SURFIX);
        for (String path : paths) {
            Map<String, String> attrs = Maps.newHashMap();

            InputStream is = store.getResource(path);
            if (is == null) {
                logger.warn("Failed to get table exd info from " + path);
                continue;
            }

            try {
                attrs.putAll(JsonUtil.readValue(is, HashMap.class));
            } finally {
                if (is != null)
                    is.close();
            }

            // parse table identity from file name
            String file = path;
            if (file.indexOf("/") > -1) {
                file = file.substring(file.lastIndexOf("/") + 1);
            }
            String tableIdentity = file.substring(0, file.length() - MetadataConstances.FILE_SURFIX.length()).toUpperCase();

            // for metadata upgrade, convert resource path to new pattern (<DB>.<TABLE>.json)
            if (tableIdentity.indexOf(".") < 0) {
                String oldResPath = TableDesc.concatExdResourcePath(tableIdentity);

                tableIdentity = appendDBName(tableIdentity);
                this.getMetadataManager().saveTableExd(tableIdentity, attrs);

                //delete old resoruce if it exists;
                if (getStore().exists(oldResPath)) {
                    getStore().deleteResource(oldResPath);
                }
            }

        }

    }

    public String appendDBName(String table) {

        if (table.indexOf(".") > 0)
            return table;

        Map<String, TableDesc> map = this.getMetadataManager().getAllTablesMap();

        int count = 0;
        String result = null;
        for (TableDesc t : map.values()) {
            if (t.getName().equalsIgnoreCase(table)) {
                result = t.getIdentity();
                count++;
            }
        }

        if (count == 1)
            return result;

        if (count > 1) {
            logger.warn("There are more than 1 table named with '" + table + "' in different database; The program couldn't determine, randomly pick '" + result + "'");
        }
        return result;
    }

    private void upgradeProjectInstance() throws IOException {
        ResourceStore store = getStore();
        List<String> paths = store.collectResourceRecursively(ResourceStore.PROJECT_RESOURCE_ROOT, ".json");

        logger.debug("Loading Project from folder " + store.getReadableResourcePath(ResourceStore.PROJECT_RESOURCE_ROOT));

        for (String path : paths) {
            path = ProjectInstance.concatResourcePath(path);

            try {
                store.getResource(path, ProjectInstance.class, ProjectManager.PROJECT_SERIALIZER);
            } catch (IOException e) {
                logger.debug("Get exception when load Project on " + path + ", going to do Project upgrade...");
                com.kylinolap.cube.model.v1.ProjectInstance oldPrj = store.getResource(path, com.kylinolap.cube.model.v1.ProjectInstance.class, new JsonSerializer<com.kylinolap.cube.model.v1.ProjectInstance>(com.kylinolap.cube.model.v1.ProjectInstance.class));

                ProjectInstance newPrj = new ProjectInstance();
                newPrj.setUuid(oldPrj.getUuid());
                newPrj.setName(oldPrj.getName());
                newPrj.setOwner(oldPrj.getOwner());
                newPrj.setDescription(oldPrj.getDescription());
                newPrj.setLastModified(oldPrj.getLastModified());
                newPrj.setLastUpdateTime(oldPrj.getLastUpdateTime());
                newPrj.setCreateTime(oldPrj.getCreateTime());
                newPrj.setStatus(oldPrj.getStatus());
                List<RealizationEntry> realizationEntries = Lists.newArrayList();
                for(String cube: oldPrj.getCubes()) {
                    RealizationEntry entry = new RealizationEntry();
                    entry.setType(RealizationType.CUBE);
                    entry.setRealization(cube);
                    realizationEntries.add(entry);
                }
                newPrj.setRealizationEntries(realizationEntries);
                
                Set<String> tables = Sets.newHashSet();
                for(String table: oldPrj.getTables()) {
                    tables.add(this.appendDBName(table));
                }
                newPrj.setTables(tables);
                
                store.putResource(newPrj.getResourcePath(), newPrj, ProjectManager.PROJECT_SERIALIZER);
            }
        }

    }


    private MetadataManager getMetadataManager() {
        return MetadataManager.getInstance(config);
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(config);
    }

    public static void main(String[] args) throws IOException {

        if (!(args != null && args.length == 1)) {
            System.out.println("Usage: java CubeMetadataUpgrade <kylin_config_folder>; e.g, /etc/kylin/");
            return;
        }

        String kylinConfigFolder = args[0];
        KylinConfig.destoryInstance();
        System.setProperty(KylinConfig.KYLIN_CONF, kylinConfigFolder);

        CubeMetadataUpgrade instance = new CubeMetadataUpgrade();

        instance.upgradeTableDesc();
        instance.upgradeTableDesceExd();
        instance.upgradeCubeDesc();
        instance.upgradeProjectInstance();
    }
}
