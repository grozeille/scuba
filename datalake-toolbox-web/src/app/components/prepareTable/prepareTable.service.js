(function() {
  'use strict';

  angular
    .module('datalakeToolbox')
    .factory('prepareTableService', prepareTableService);

  /** @ngInject */
  function prepareTableService($log, $http, $location, $filter, $q, $rootScope, hiveService) {

    var vm = this;
    vm.apiHost = $location.protocol() +"://"+$location.host() +":"+$location.port()+"/api";
    //vm.apiHost = "http://localhost:8000/api";



    vm.cancelCurrentGetData = null;

    vm.getServiceData = function(response) {
      return response.data;
    };

    vm.catchServiceException = function(error) {
      $log.error('XHR Failed for getContributors.\n' + angular.toJson(error.data, true));
    };



    function getData(maxRows){

      if(vm.cancelCurrentGetData != null){
        vm.cancelCurrentGetData("new Get Data");
      }
      vm.cancelCurrentGetData = null;

      if(angular.isUndefined(maxRows)){
        maxRows = 10000;
      }

      if(Object.keys(vm.tables).length == 0){
        return $q.when([]);
      }

      var result = hiveService.getData(buildDataSetConf(), maxRows);

      vm.cancelCurrentGetData = result.cancel;
      return result.promise;
    }

    function cancelGetData(){
      if(vm.cancelCurrentGetData != null){
        vm.cancelCurrentGetData("user cancellation");
      }
      vm.cancelCurrentGetData = null;
    };

    function getExcelWorksheets(){

    };


    var service = {
      getData: getData,
      cancelGetData: cancelGetData,
      getExcelWorksheets: getExcelWorksheets
    };

    return service;

  }
})();
