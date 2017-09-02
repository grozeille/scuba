module.exports = prepareTableService;

/** @ngInject */
function prepareTableService($log, $http, $location, $filter, $q, $rootScope, Upload) {
  var vm = this;
  vm.apiHost = $location.protocol() + '://' + $location.host() + ':' + $location.port() + '/api';

  vm.getServiceData = function(response) {
    return response.data;
  };

  vm.catchServiceException = function(error) {
    $log.error('XHR Failed for getContributors.\n' + angular.toJson(error.data, true));
  };

  function getRawData(file) {
    var upload = Upload.upload({
      url: vm.apiHost + '/hive/data/raw',
      data: {file: file}
    });

    return upload
      .then(vm.getServiceData)
      .catch(vm.catchServiceException);
  }

  function getCsvData(data) {
    var upload = Upload.upload({
      url: vm.apiHost + '/hive/data/csv',
      data: data
    });

    return upload
      .then(vm.getServiceData)
      .catch(vm.catchServiceException);
  }

  function getExcelData(data) {
    var upload = Upload.upload({
      url: vm.apiHost + '/hive/data/excel',
      data: data
    });

    return upload
      .then(vm.getServiceData)
      .catch(vm.catchServiceException);
  }

  function getExcelWorksheets(file) {
    var upload = Upload.upload({
      url: vm.apiHost + '/hive/data/excel/sheets',
      data: {file: file}
    });

    return upload
      .catch(vm.catchServiceException);
  }

  function upload(options) {
  }

  function save(options) {
    var baseName = 'app_aaa';
    var url = vm.apiHost + '/hive/tables/' + baseName + '/' + options.name;

    var creationRequest = {
      comment: options.description,
      dataDomainOwner: 'mathias.kluba@gmail.com',
      tags: ['aaa']
    };

    return $http.put(url, creationRequest)
      .then(function() {
        var uploadUrl = url + '/data/' + options.format;

        var data = {};
        if(options.format === 'csv') {
          data = options.csvOptions;
        }
        else if(options.format === 'excel') {
          data = options.excelOptions;
        }
        data.file = options.file;

        var upload = Upload.upload({
          url: uploadUrl,
          data: data
        });

        return upload
          .then(vm.getServiceData)
          .catch(vm.catchServiceException);
      })
      .catch(vm.catchServiceException);
  }

  var service = {
    getExcelData: getExcelData,
    getCsvData: getCsvData,
    getRawData: getRawData,
    getExcelWorksheets: getExcelWorksheets,
    save: save
  };

  return service;
}
