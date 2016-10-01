(function() {
  'use strict';

  angular
    .module('datalakeToolbox')
    .factory('prepareTableService', prepareTableService);

  /** @ngInject */
  function prepareTableService($log, $http, $location, $filter, $q, $rootScope, hiveService, Upload) {

    var vm = this;
    vm.apiHost = $location.protocol() +"://"+$location.host() +":"+$location.port()+"/api";
    //vm.apiHost = "http://localhost:8000/api";


    vm.getServiceData = function(response) {
      return response.data;
    };

    vm.catchServiceException = function(error) {
      $log.error('XHR Failed for getContributors.\n' + angular.toJson(error.data, true));
    };

    function getRawData(file){
      var upload = Upload.upload({
        url: vm.apiHost+"/hive/raw/data",
        data: { file: file}
      });

      return upload
        .then(vm.getServiceData)
        .catch(vm.catchServiceException);
    }

    function getCsvData(data){
      var upload = Upload.upload({
        url: vm.apiHost+"/hive/csv/data",
        data: data
      });

      return upload
        .then(vm.getServiceData)
        .catch(vm.catchServiceException);
    }

    function getExcelData(data){

      var upload = Upload.upload({
        url: vm.apiHost+"/hive/excel/data",
        data: data
      });

      return upload
        .then(vm.getServiceData)
        .catch(vm.catchServiceException);
    }

    function getExcelWorksheets(file){

      var upload = Upload.upload({
        url: vm.apiHost+"/hive/excel/sheets",
        data: { file: file}
      });

      return upload
        .then(vm.getServiceData)
        .catch(vm.catchServiceException);
    }


    var service = {
      getExcelData: getExcelData,
      getCsvData: getCsvData,
      getRawData: getRawData,
      getExcelWorksheets: getExcelWorksheets
    };

    return service;

  }
})();
