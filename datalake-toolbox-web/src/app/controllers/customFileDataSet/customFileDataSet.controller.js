require('./customFileDataSet.css');
require('bootstrap-filestyle');

module.exports = {
  controller: CustomFileDataSetController,
  controllerAs: 'customFileDataSet',
  template: require('./customFileDataSet.html')
};

/** @ngInject */
function CustomFileDataSetController($timeout, $log, $location, $filter, $scope, $stateParams, $state, userService, customFileDataSetService) {
  var vm = this;
  vm.isLoading = false;

  vm.databaseAndTable = $stateParams.databaseAndTable;
  if(angular.isDefined(vm.database_table) && vm.databaseAndTable !== '') {
    // TODO load existing table
  }

  vm.database = '';
  vm.name = '';
  vm.description = '';
  vm.dataType = '';
  vm.csvSeparator = 'comma';
  vm.csvSeparatorCustom = '';
  vm.csvTextQualifier = 'doublequote';
  vm.csvFirstLineHeader = 'true';
  vm.fileInfo = {};
  vm.excelSource = '';
  vm.excelSheets = [];
  vm.excelFirstLineHeader = 'true';
  vm.temporaryTableName = '';
  vm.fileUploaded = false;
  vm.maxLinePreview = 5000;

  vm.jsTags = {
    edit: true,
    texts: {
      inputPlaceHolder: 'Type text here'
    }
  };

  vm.gridOptions = {
    enableSorting: false,
    enableColumnMenus: false,
    enableColumnResizing: true,
    appScopeProvider: vm,
    columnDefs: [],
    data: [],
    onRegisterApi: function(gridApi) {
      vm.gridSampleApi = gridApi;
    }
  };

  $scope.$watch(function(scope) {
    return(vm.fileInfo);
  }, function(newValue, oldValue) {
    if(newValue === null || angular.isUndefined(newValue.name)) {
      return;
    }

    vm.fileUploaded = false;

    var extension = newValue.name.split('.').pop();
    var extensionStart = extension.substr(0, 3);
    if(extensionStart.localeCompare('xls') === 0) {
      vm.dataType = 'excel';
    }
    else if(extension.localeCompare('csv') === 0 || extension.localeCompare('tsv') === 0 || extension.localeCompare('txt') === 0) {
      vm.dataType = 'csv';
    }
    else {
      vm.dataType = 'raw';
    }
  });

  vm.getWorksheet = function() {
    var options = {
      database: vm.database,
      name: vm.temporaryTableName
    };

    customFileDataSetService.getExcelWorksheets(options).then(function(data) {
      vm.excelSheets = data;
      if(vm.excelSheets.length > 0) {
        vm.excelSource = vm.excelSheets[0];
      }
    });
  };

  function getSeparator() {
    var separator = '';
    if(vm.csvSeparator === 'semicolon') {
      separator = ';';
    } else if(vm.csvSeparator === 'comma') {
      separator = ',';
    } else if(vm.csvSeparator === 'tab') {
      separator = '\t';
    } else if(vm.csvSeparator === 'space') {
      separator = ' ';
    } else if(vm.csvSeparator === 'custom') {
      separator = vm.csvSeparatorCustom;
    }

    return separator;
  }

  function getTextQualifier() {
    var textQualifier = '';
    if(vm.csvTextQualifier === 'doublequote') {
      textQualifier = '"';
    } else if(vm.csvTextQualifier === 'simplequote') {
      textQualifier = '\'';
    }
    return textQualifier;
  }

  vm.getData = function() {
    vm.isLoading = true;
    vm.gridOptions.columnDefs = [];
    vm.gridOptions.data = [];

    var loadData = function(data) {
      if(data !== null) {
        vm.gridOptions.data = data.data;
      }
    };

    var stopLoading = function() {
      vm.isLoading = false;
    };

    var parseData = function() {
      if(vm.dataType === 'excel') {
        var excelOptions = {
          database: vm.database,
          name: vm.temporaryTableName,
          maxLinePreview: vm.maxLinePreview,
          sheet: vm.excelSource,
          firstLineHeader: vm.excelFirstLineHeader
        };

        return customFileDataSetService.getExcelData(excelOptions)
          .then(loadData)
          .then(stopLoading)
          .catch(stopLoading);
      }
      if(vm.dataType === 'csv') {
        var separator = getSeparator();

        var textQualifier = getTextQualifier();

        var csvOptions = {
          database: vm.database,
          name: vm.temporaryTableName,
          maxLinePreview: vm.maxLinePreview,
          separator: separator,
          textQualifier: textQualifier,
          firstLineHeader: vm.csvFirstLineHeader
        };

        return customFileDataSetService.getCsvData(csvOptions)
          .then(loadData)
          .then(stopLoading)
          .catch(stopLoading);
      }
      if(vm.dataType === 'raw') {
        var rawOptions = {
          database: vm.database,
          name: vm.temporaryTableName,
          maxLinePreview: vm.maxLinePreview
        };

        return customFileDataSetService.getRawData(rawOptions)
          .then(loadData)
          .then(stopLoading)
          .catch(stopLoading);
      }
    };

    // upload the file if not already deployed
    if(vm.fileUploaded === false) {
      return customFileDataSetService.uploadFile({
        database: vm.database,
        name: vm.temporaryTableName,
        file: vm.fileInfo
      }).then(function() {
        vm.fileUploaded = true;
        return parseData();
      });
    }
    else {
      return parseData();
    }
  };

  vm.save = function() {
    var saveTableRequest = {
      database: vm.database,
      name: vm.name,
      description: vm.description,
      temporary: false
    };

    customFileDataSetService.saveTable(saveTableRequest).then(function() {
      var uploadRequest = {
        database: vm.database,
        name: vm.name,
        file: vm.fileInfo
      };

      // update file
      customFileDataSetService.uploadFile(function() {
        // update the data
        if(vm.dataType === 'raw') {
          options.rawOptions = {
            database: vm.database,
            name: vm.name
          };

          customFileDataSetService.saveDataAsRaw(options);
        }
        else if(vm.dataType === 'csv') {
          options.csvOptions = {
            database: vm.database,
            name: vm.name,
            separator: getSeparator(),
            textQualifier: getTextQualifier(),
            firstLineHeader: vm.csvFirstLineHeader
          };

          customFileDataSetService.saveDataAsCsv(options);
        }
        else if(vm.dataType === 'excel') {
          options.excelOptions = {
            database: vm.database,
            name: vm.name,
            sheet: vm.excelSource,
            firstLineHeader: vm.excelFirstLineHeader
          };

          customFileDataSetService.saveDataAsExcel(options);
        }
      });
    });

    var options = {
      file: vm.fileInfo,
      name: vm.name,
      description: vm.description,
      format: vm.dataType
    };

    if(vm.dataType === 'csv') {
      options.csvOptions = {
        separator: getSeparator(),
        textQualifier: getTextQualifier(),
        firstLineHeader: vm.csvFirstLineHeader
      };
    }
    else if(vm.dataType === 'excel') {
      options.excelOptions = {
        sheet: vm.excelSource,
        firstLineHeader: vm.excelFirstLineHeader
      };
    }

    return customFileDataSetService.save(options)
        .then(function() {
          $state.go('dataSet');
        });
  };

  function activate() {
    userService.getLastProject().then(function(data) {
      $log.info(data);
      vm.database = data.hiveDatabase;

      userService.getCurrent().then(function(data) {
        var today = new Date();
        var minutes = today.getMinutes();
        var day = today.getDate();
        var month = today.getMonth() + 1; // January is 0!
        var year = today.getFullYear();

        if(minutes < 10) {
          minutes = '0' + minutes;
        }

        if(day < 10) {
          day = '0' + day;
        }

        if(month < 10) {
          month = '0' + month;
        }

        today = year + month + day + minutes;

        vm.temporaryTableName = 'new_file_' + data.login + '_' + today;

        // create the temporary table
        var saveTableRequest = {
          database: vm.database,
          name: vm.temporaryTableName,
          temporary: true
        };

        customFileDataSetService.saveTable(saveTableRequest);
      });
    });

    $(':file').filestyle();
  }

  activate();
}
