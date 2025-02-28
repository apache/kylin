export default {
  en: {
    dialogType: 'Storage Setting',
    storageType: 'Storage type',
    optionFixed: 'Fixed partition storage',
    optionFlexiable: 'Flexible partition storage',
    optionFixedTips: 'Fixed partition storage must store data according to model partition settings and filter data in a fixed way to speed up queries.',
    optionFlexiableTips: 'Flexible partition storage provides a more flexible storage partitioning method. You can set partition columns, Z-order columns, etc. at the index level based on data characteristics and query modes to help improve query performance in specific scenarios.',
    updateSuccessful: 'Successfully updated'
  },
  'zh-cn': {
    dialogType: '存储设置',
    storageType: '存储类型',
    optionFixed: '固定分区存储',
    optionFlexiable: '灵活分区存储',
    optionFixedTips: '固定分区存储必须按照模型分区设置存储数据，根据固定的方式过滤数据加速查询。',
    optionFlexiableTips: '灵活分区存储提供更灵活的存储分区方式。您可以根据数据特征和查询模式，在索引级进行分区列、Z-order 列等设置，帮助提升特定场景的查询性能。',
    updateSuccessful: '更新成功'
  }
}
