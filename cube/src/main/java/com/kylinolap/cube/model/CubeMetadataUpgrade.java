package com.kylinolap.cube.model;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
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

    private List<String> updatedResources = Lists.newArrayList();
    private List<String> errorMsgs = Lists.newArrayList();

    private static final Log logger = LogFactory.getLog(CubeMetadataUpgrade.class);

    public CubeMetadataUpgrade(String newMetadataUrl) {
        KylinConfig.destoryInstance();
        System.setProperty(KylinConfig.KYLIN_CONF, newMetadataUrl);
        KylinConfig.getInstanceFromEnv().setMetadataUrl(newMetadataUrl);

        
        config = KylinConfig.getInstanceFromEnv();
        store = getStore();
    }
    
    public void upgrade() {

        upgradeTableDesc();
        upgradeTableDesceExd();
        upgradeCubeDesc();
        upgradeProjectInstance();
        
        verify();
        
    }
    
    private void verify() {
        MetadataManager.getInstance(config).reload();
        CubeDescManager.clearCache();
        CubeDescManager.getInstance(config);
    }

    private List<String> listResourceStore(String pathRoot) {
        List<String> paths = null;
        try {
            paths = store.collectResourceRecursively(pathRoot, MetadataConstances.FILE_SURFIX);
        } catch (IOException e1) {
            e1.printStackTrace();
            errorMsgs.add("Get IOException when scan resource store at: " + ResourceStore.CUBE_DESC_RESOURCE_ROOT);
        }

        return paths;
    }

    private void upgradeCubeDesc() {
        logger.info("Reloading Cube Metadata from folder " + store.getReadableResourcePath(ResourceStore.CUBE_DESC_RESOURCE_ROOT));

        List<String> paths = listResourceStore(ResourceStore.CUBE_DESC_RESOURCE_ROOT);
        for (String path : paths) {

            try {
                CubeDescUpgrader upgrade = new CubeDescUpgrader(path);
                CubeDesc ndesc = upgrade.upgrade();
                ndesc.setSignature(ndesc.calculateSignature());
                
                getStore().putResource(ndesc.getModel().getResourcePath(), ndesc.getModel(), MetadataManager.MODELDESC_SERIALIZER);
                getStore().putResource(ndesc.getResourcePath(), ndesc, CubeDescManager.CUBE_DESC_SERIALIZER);
                updatedResources.add(ndesc.getResourcePath());
            } catch (IOException e) {
                e.printStackTrace();
                errorMsgs.add("Upgrade CubeDesc at '" + path + "' failed: " + e.getLocalizedMessage());
            }
        }

    }

    private void upgradeTableDesc() {
        List<String> paths = listResourceStore(ResourceStore.TABLE_RESOURCE_ROOT);
        for (String path : paths) {
            TableDesc t;
            try {
                t = store.getResource(path, TableDesc.class, MetadataManager.TABLE_SERIALIZER);
                t.init();

                // if it only has 1 "." in the path, delete the old resource if it exists
                if (path.substring(path.indexOf(".")).length() == MetadataConstances.FILE_SURFIX.length()) {
                    getStore().deleteResource(path);
                    // the new source will be new;
                    t.setLastModified(0);
                    getStore().putResource(t.getResourcePath(), t, MetadataManager.TABLE_SERIALIZER);
                    updatedResources.add(t.getResourcePath());
                }
            } catch (IOException e) {
                e.printStackTrace();
                errorMsgs.add("Upgrade TableDesc at '" + path + "' failed: " + e.getLocalizedMessage());
            }

        }

    }

    private void upgradeTableDesceExd() {

        List<String> paths = listResourceStore(ResourceStore.TABLE_EXD_RESOURCE_ROOT);
        for (String path : paths) {
            Map<String, String> attrs = Maps.newHashMap();

            InputStream is = null;
            try {
                is = store.getResource(path);
                if (is == null) {
                    continue;
                }
                try {
                    attrs.putAll(JsonUtil.readValue(is, HashMap.class));
                } finally {
                    if (is != null)
                        is.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
                errorMsgs.add("Upgrade TableDescExd at '" + path + "' failed: " + e.getLocalizedMessage());
            }

            // parse table identity from file name
            String file = path;
            if (file.indexOf("/") > -1) {
                file = file.substring(file.lastIndexOf("/") + 1);
            }
            String tableIdentity = file.substring(0, file.length() - MetadataConstances.FILE_SURFIX.length()).toUpperCase();

            // for metadata upgrade, convert resource path to new pattern (<DB>.<TABLE>.json)
            if (tableIdentity.indexOf(".") < 0) {
                tableIdentity = appendDBName(tableIdentity);
                try {
                    getMetadataManager().saveTableExd(tableIdentity, attrs);
                    //delete old resoruce if it exists;
                    getStore().deleteResource(path);
                    updatedResources.add(path);
                } catch (IOException e) {
                    e.printStackTrace();
                    errorMsgs.add("Upgrade TableDescExd at '" + path + "' failed: " + e.getLocalizedMessage());
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
            errorMsgs.add("There are more than 1 table named with '" + table + "' in different database; The program couldn't determine, randomly pick '" + result + "'");
        }
        return result;
    }

    private void upgradeProjectInstance() {
        List<String> paths = listResourceStore(ResourceStore.PROJECT_RESOURCE_ROOT);
        for (String path : paths) {
            try {
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
                for (String cube : oldPrj.getCubes()) {
                    RealizationEntry entry = new RealizationEntry();
                    entry.setType(RealizationType.CUBE);
                    entry.setRealization(cube);
                    realizationEntries.add(entry);
                }
                newPrj.setRealizationEntries(realizationEntries);
                newPrj.getCreateTimeUTC();

                Set<String> tables = Sets.newHashSet();
                for (String table : oldPrj.getTables()) {
                    tables.add(this.appendDBName(table));
                }
                newPrj.setTables(tables);

                store.putResource(newPrj.getResourcePath(), newPrj, ProjectManager.PROJECT_SERIALIZER);
                updatedResources.add(path);
            } catch (IOException e) {
                e.printStackTrace();
                errorMsgs.add("Upgrade Project at '" + path + "' failed: " + e.getLocalizedMessage());
            }
        }

    }

    private MetadataManager getMetadataManager() {
        return MetadataManager.getInstance(config);
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(config);
    }

    public static void main(String[] args) {

        if (!(args != null && args.length == 1)) {
            System.out.println("Usage: java CubeMetadataUpgrade <metadata_export_folder>; e.g, /export/kylin/meta");
            return;
        }

        String exportFolder = args[0];
        
        File oldMetaFolder = new File(exportFolder);
        if(!oldMetaFolder.exists()) {
            System.out.println("Provided folder doesn't exist: '" + exportFolder + "'");
            return;
        }
        
        if(!oldMetaFolder.isDirectory()) {
            System.out.println("Provided folder is not a directory: '" + exportFolder + "'");
            return;
        }

        
        String newMetadataUrl = oldMetaFolder.getAbsolutePath() + "_v2";
        try {
            FileUtils.deleteDirectory(new File(newMetadataUrl));
            FileUtils.copyDirectory(oldMetaFolder, new File(newMetadataUrl));
        } catch (IOException e) {
            e.printStackTrace();
        }

        CubeMetadataUpgrade instance = new CubeMetadataUpgrade(newMetadataUrl);

        instance.upgrade();
        logger.info("=================================================================");
        logger.info("Run CubeMetadataUpgrade completed; The following resources have been successfully updated in : " + newMetadataUrl);
        for (String s : instance.updatedResources) {
            logger.info(s);
        }

        logger.info("=================================================================");
        if (instance.errorMsgs.size() > 0) {
            logger.info("Here are the error/warning messages, you may need check:");
            for (String s : instance.errorMsgs) {
                logger.warn(s);
            }
        } else {
            logger.info("No error or warning messages; The migration is success.");
        }
    }
}
