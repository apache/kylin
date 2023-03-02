export default {
  'en': {
    capbility: 'Favorite Rate',
    status: 'Status',
    streamingTips: '流数据模型，部分功能不可用',
    modelPartitionSet: 'Model Partition',
    enableModelTip: 'Are you sure you want to online the model {modelName}?',
    enableModelTitle: 'Online Model',
    noModel: 'You can click below button to add a model',
    datacheck: 'Data Check',
    favorite: 'Favorite',
    importMdx: 'Import MDX',
    exportTds: 'Export TDS',
    exportMdx: 'Export MDX',
    rename: 'Rename',
    delete: 'Delete',
    purge: 'Purge',
    disable: 'Disable',
    enable: 'Enable',
    recommendationsTiTle: 'Recommendations',
    inputModelName: 'Please enter new model name',
    inputCloneName: 'Please enter clone model name',
    segment: 'Segment',
    aggregateGroup: 'Aggregate Group',
    tableIndex: 'Table Index',
    indexOverview: 'Index Overview',
    onLine: 'Online',
    offLine: 'Offline',
    build: 'Build Index',
    buildTitle: 'Build',
    storage: 'Storage',
    usage: 'Usage',
    rowCount: 'Rows',
    secStorage: 'Tiered Storage',
    fullLoadDataTitle: 'Load Data',
    fullLoadDataContent1: 'The model {modelName} has no partition.',
    fullLoadDataContent2: '{storageSize} storage would be required as all data would be reloaded.',
    fullLoadDataContent3: 'Are you sure you want to reload the data?',
    fullScreen: 'Full Screen',
    exitFullScreen: 'Exit Full Screen',
    usageTip: 'Times of the {mode} used by queries',
    model: 'model',
    indexGroup: 'index group',
    expansionRate: 'Expansion Rate',
    expansionRateTip: 'Expansion Rate = Storage Size / Source Table Size<br/>Note: The expansion rate won\'t show if the storage size is less than 1 GB.',
    expansionTip: 'The expansion rate won’t show if the storage size is less than 1 GB',
    tentative: 'Unknown',
    tentativeTips: 'The source table data is empty, the expansion rate is unknown',
    recommendations: 'Recommendation',
    recommendationsTip: 'By analyzing the query history and model usage, the system will provide some recommendations.',
    clearAll: 'Clear All',
    ALL: 'All',
    ONLINE: 'ONLINE',
    OFFLINE: 'OFFLINE',
    BROKEN: 'BROKEN',
    status_c: 'Status: ',
    modelType_c: 'Model Attributes: ',
    others: 'Others',
    reset: 'Reset',
    lastModifyTime_c: 'Last Updated Time: ',
    lastBuildTime: 'Last Build Time: ',
    allTimeRange: 'All Time Range',
    filterButton: 'Filter',
    aggIndexCount: 'Index Amount',
    recommendations_c: 'Recommendation: ',
    clickToView: 'Review',
    filterModelOrOwner: 'Search model name',
    autoFix: 'automatic fix',
    ID: 'ID',
    column: 'Column',
    sort: 'Order',
    buildTips: 'The indexes in this model have not been built and are not ready for serving queries. Please build indexes to optimize query performance.',
    iKnow: 'Got it',
    exportMetadata: 'Export Metadata',
    noModelsExport: 'The model\'s metadata can\'t be exported as there are no models yet.',
    exportMetadatas: 'Export Model',
    exportMetadataSuccess: 'A model metadata package is being generated. Download will start automatically once ready.',
    exportMetadataFailed: 'Can\'t export models at the moment. Please try again.',
    bokenModelExportMetadatasTip: 'Can\'t export model file at the moment as the model is BROKEN',
    importModels: 'Import Model',
    guideToAcceptRecom: 'You may click on the recommendations besides the model name to view how to optimize.',
    overview: 'Overview',
    changeModelOwner: 'Change Owner',
    change: 'Change',
    modelName: 'Model',
    modelType: 'Types',
    STREAMING: 'Streaming Model',
    BATCH: 'Batch Model',
    HYBRID: 'Fusion Model',
    changeTo: 'Change Owner To',
    pleaseChangeOwner: 'Please change model owner',
    changeDesc: 'You can change the owner of the model to a system admin, a user in the project ADMIN role, or a user in the project management role.',
    buildIndex: 'Build Index',
    batchBuildSubTitle: 'Please choose which data ranges you\'d like to build with the added indexes.',
    refrashWarningSegment: 'Only ONLINE segments could be refreshed',
    closeSCD2ModalOnlineTip: 'This model can\'t go online as it includes non-equal join conditions(≥, <). Please delete those join conditions, or turn on `Support History table` in project settings.',
    storageTip: 'Calculates the amount of data built in this model',
    subPartitionValuesManage: 'Manage Sub-Partition Values',
    segmentHoletips: 'There exists a gap in the segment range, and the data of this range cannot be queried. Please confirm whether to add the following segments to fix.',
    fixSegmentTitle: 'Fix Segment'
  }
}
