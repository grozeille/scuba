module.exports = customFileDataSetService;

/** @ngInject */
function customFileDataSetService($log, $http, $location, $filter, $q, $rootScope, Upload) {
  var vm = this;
  vm.apiHost = $location.protocol() + '://' + $location.host() + ':' + $location.port() + '/api';

  vm.getServiceData = function(response) {
    return response.data;
  };

  vm.catchServiceException = function(error) {
    $log.error('XHR Failed.\n' + angular.toJson(error.data, true));
    throw error;
  };

  function initDataSet(database, table) {

  }

  function cloneDataSet(database, table) {

  }

  function saveDataSet() {

  }

  function getRawData(options) {
    var url = vm.apiHost + '/dataset/custom-file/' + options.database + '/' + options.name + '/file/parse-data';

    var parseRequest = {
      format: 'RAW',
      maxLinePreview: options.maxLinePreview
    };

    return $http.post(url, parseRequest)
      .then(vm.getServiceData)
      .catch(vm.catchServiceException);
  }

  function saveDataAsRaw(options) {
    var url = vm.apiHost + '/dataset/custom-file/' + options.database + '/' + options.name + '/update';

    var updateRequest = {
      format: 'RAW'
    };

    return $http.post(url, updateRequest)
      .catch(vm.catchServiceException);
  }

  function getCsvData(options) {
    var url = vm.apiHost + '/dataset/custom-file/' + options.database + '/' + options.name + '/file/parse-data';

    var parseRequest = {
      format: 'CSV',
      maxLinePreview: options.maxLinePreview,
      separator: options.separator,
      textQualifier: options.textQualifier,
      firstLineHeader: options.firstLineHeader
    };

    return $http.post(url, parseRequest)
      .then(vm.getServiceData)
      .catch(vm.catchServiceException);
  }

  function saveDataAsCsv(options) {
    var url = vm.apiHost + '/dataset/custom-file/' + options.database + '/' + options.name + '/update';

    var updateRequest = {
      format: 'CSV',
      separator: options.separator,
      textQualifier: options.textQualifier,
      firstLineHeader: options.firstLineHeader
    };

    return $http.post(url, updateRequest)
      .catch(vm.catchServiceException);
  }

  function getExcelData(options) {
    var url = vm.apiHost + '/dataset/custom-file/' + options.database + '/' + options.name + '/file/parse-data';

    var parseRequest = {
      format: 'EXCEL',
      maxLinePreview: options.maxLinePreview,
      sheet: options.sheet,
      firstLineHeader: options.firstLineHeader
    };

    return $http.post(url, parseRequest)
      .then(vm.getServiceData)
      .catch(vm.catchServiceException);
  }

  function saveDataAsExcel(options) {
    var url = vm.apiHost + '/dataset/custom-file/' + options.database + '/' + options.name + '/update';

    var updateRequest = {
      format: 'EXCEL',
      sheet: options.sheet,
      firstLineHeader: options.firstLineHeader
    };

    return $http.post(url, updateRequest)
      .catch(vm.catchServiceException);
  }

  function getExcelWorksheets(options) {
    var url = vm.apiHost + '/dataset/custom-file/' + options.database + '/' + options.name + '/file/sheets';

    return $http.get(url)
      .then(vm.getServiceData)
      .catch(vm.catchServiceException);
  }

  function uploadFile(options) {
    var url = vm.apiHost + '/dataset/custom-file/' + options.database + '/' + options.name + '/file';

    var upload = Upload.upload({
      url: url,
      data: {file: options.file},
      method: 'PUT'
    });

    return upload
      .catch(vm.catchServiceException);
  }

  function saveTable(options) {
    var url = vm.apiHost + '/dataset/custom-file/' + options.database + '/' + options.name;

    var creationRequest = {
      comment: options.description,
      tags: ['aaa'],
      temporary: options.temporary
    };

    return $http.put(url, creationRequest)
      .catch(vm.catchServiceException);
  }

  var service = {
    saveTable: saveTable,
    saveDataAsExcel: saveDataAsExcel,
    saveDataAsCsv: saveDataAsCsv,
    saveDataAsRaw: saveDataAsRaw,
    getExcelData: getExcelData,
    getExcelWorksheets: getExcelWorksheets,
    getCsvData: getCsvData,
    getRawData: getRawData,
    uploadFile: uploadFile
  };

  return service;
}
