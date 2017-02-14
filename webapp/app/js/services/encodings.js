/**
 * Created by luguosheng on 17/2/9.
 */
KylinApp.factory('EncodingService', ['$resource', function ($resource, config) {
  return $resource(Config.service.url + 'encodings/valid_encodings', {}, {
    getEncodingMap: {method: 'GET', params: {}, isArray: false}
  });
}]);
