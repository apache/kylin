export default {
  'en': {
    tableName: 'Table Name:',
    partitionKey: 'Time Partition:',
    loadInterval: 'Load Interval:',
    storageType: 'Storage Type:',
    storageSize: 'Storage Size:',
    totalRecords: 'Loaded Records:',
    rows: 'Rows',
    noPartition: 'No Partition',
    changePartitionTitle: 'Change Partition',
    changePartitionContent1: 'You\'re going to change the table {tableName}\'s partition from {oldPartitionKey} to {newPartitionKey}.',
    changePartitionContent2: 'The change may clean loaded storage {storageSize} and re-load data.',
    changePartitionContent3: 'Do you really need to change the partition?',
    fullLoadDataTitle: 'Load Data',
    fullLoadDataContent1: 'Because the table {tableName} has no partition, the load data job will reload all impacted data {storageSize}.',
    fullLoadDataContent2: 'Do you need to submit the load data job?',
    loadSuccessTip: 'Success to submit the load data job. Please view it on the monitor page.',
    loadRange: 'Loaded Range',
    loadData: 'Load Data',
    refreshData: 'Refresh Data',
    notLoadYet: 'Not loaded yet',
    suggestSetLoadRangeTip: 'Please enter a load range after the partition set, or the system wouldn\'t accelerate queries automatically.',
    dateFormat: 'Time Format:',
    detectFormat: 'Detect partition format'
  }
}
