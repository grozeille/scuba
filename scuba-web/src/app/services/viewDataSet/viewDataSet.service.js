module.exports = viewDataSetService;

/** @ngInject */
function viewDataSetService($log, $http, $location, $filter, $q, $rootScope, dataSetService) {
  var vm = this;
  vm.apiHost = $location.protocol() + '://' + $location.host() + ':' + $location.port() + '/api';

  vm.database = '';
  vm.table = '';
  vm.dataSet = {
    comment: '',
    tags: []
  };

  vm.getServiceData = function(response) {
    return response.data;
  };

  vm.catchServiceException = function(error) {
    $log.error('XHR Failed.\n' + angular.toJson(error.data, true));
    throw error;
  };

  function initDataSet(database, table) {
    vm.database = database;
    vm.table = table;
    vm.dataSet = null;

    return dataSetService.getDataSet(vm.database, vm.table)
    .then(function(data) {
      vm.dataSet = {
        comment: data.comment,
        tags: data.tags
      };
    });
  }

  function getDataSet() {
    return {
      database: vm.database,
      name: vm.table,
      comment: vm.dataSet.comment,
      tags: vm.dataSet.tags
    };
  }

  function getData(max) {
    return $http.get(vm.apiHost + '/dataset/' + vm.database + '/' + vm.table + '/data?max=' + max + '&useTablePrefix=false')
      .then(vm.getServiceData)
      .catch(vm.catchServiceException);
  }

  var service = {
    initDataSet: initDataSet,
    getDataSet: getDataSet,
    getData: getData
  };

  return service;
}
