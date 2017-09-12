module.exports = dataSetService;

/** @ngInject */
function dataSetService($log, $http, $location, $filter, $q, $rootScope) {
  var vm = this;
  vm.apiHost = $location.protocol() + '://' + $location.host() + ':' + $location.port() + '/api';

  vm.getServiceData = function(response) {
    return response.data;
  };

  vm.catchServiceException = function(error) {
    $log.error('XHR Failed.\n' + angular.toJson(error.data, true));
    throw error;
  };

  function getAllDataSet() {
    return $http.get(vm.apiHost + '/dataset')
      .then(vm.getServiceData)
      .catch(vm.catchServiceException);
  }

  function getDataSet(database, table) {
    return $http.get(vm.apiHost + '/dataset/' + database + '/' + table)
      .then(vm.getServiceData)
      .catch(vm.catchServiceException);
  }

  function deleteDataSet(database, table) {
    return $http.delete(vm.apiHost + '/dataset/' + database + '/' + table)
      .catch(vm.catchServiceException);
  }

  var service = {
    getAllDataSet: getAllDataSet,
    getDataSet: getDataSet,
    deleteDataSet: deleteDataSet
  };

  return service;
}
