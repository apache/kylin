export const mockDatasourceArray = [
  {
    name: 'Default',
    sourceType: process.env.NODE_ENV === 'development' ? 11 : 9,
    type: 'Hive',
    host: '127.0.0.1',
    port: '8080',
    createTime: new Date().getTime()
  }
]
