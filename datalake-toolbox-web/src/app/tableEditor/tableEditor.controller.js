(function() {
  'use strict';

  angular
    .module('datalakeToolbox')
    .controller('TableEditorController', TableEditorController);

  /** @ngInject */
  function TableEditorController($timeout, $log, $location, $filter, $scope, prepareTableService) {
    var vm = this;
    vm.isLoading = false;

    vm.name = "";
    vm.description = "";
    vm.dataType = "";
    vm.csvSeparator = "semicolon";
    vm.csvSeparatorCustom = "";
    vm.csvTextQualifier = "";
    vm.csvFirstLineHeader = "true";
    vm.fileInfo = {};
    vm.excelSource = "";
    vm.excelSheets = [];
    vm.excelFirstLineHeader = "true";

    var columnDefs = [];
    /*columnDefs.push({
      field: "aaa",
      enableHiding: false,
      minWidth: 70,
      width: 100,
      enableColumnResizing: true
    });*/

    vm.gridOptions = {
      enableSorting: false,
      enableColumnMenus: false,
      enableColumnResizing: true,
      appScopeProvider: vm,
      columnDefs: columnDefs,
      data : [ ],
      onRegisterApi: function( gridApi ) {
        vm.gridSampleApi = gridApi;
      }
    };

    $scope.$watch(function(scope){
      return(vm.fileInfo);
    }, function( newValue, oldValue ) {
      if(newValue == null || angular.isUndefined(newValue.name)){
        return;
      }

      var extension = newValue.name.split(".").pop();
      var extensionStart = extension.substr(0, 3);
      if(extensionStart.localeCompare("xls") == 0){
        vm.dataType = "excel";
      }
      else if(extension.localeCompare("csv") == 0 || extension.localeCompare("tsv") == 0 || extension.localeCompare("txt") == 0){
        vm.dataType = "csv";
      }
      else {
        vm.dataType = "raw";
      }
    });


    vm.getWorksheet = function(){
      prepareTableService.getExcelWorksheets(vm.fileInfo).then(function(data){
        vm.excelSheets = data;
        if(vm.excelSheets.length > 0){
          vm.excelSource = vm.excelSheets[0];
        }
      });

    };


    vm.getData = function(){
      vm.isLoading = true;

      var loadData = function(data){
        if(data != null){
          vm.gridOptions = {
                enableSorting: false,
                enableColumnMenus: false,
                enableColumnResizing: true,
                appScopeProvider: vm,
                columnDefs: columnDefs,
                data : [ ],
                onRegisterApi: function( gridApi ) {
                  vm.gridSampleApi = gridApi;
                }
              };
          vm.gridOptions.data = data.data;
        }
      };

      var stopLoading = function(){
        vm.isLoading = false;
      };

      if(vm.dataType == 'excel'){
        var options = {
          file: vm.fileInfo,
          sheet: vm.excelSource,
          firstLineHeader: vm.excelFirstLineHeader
        };

        return prepareTableService.getExcelData(options)
          .then(loadData)
          .then(stopLoading)
          .catch(stopLoading);
      }
      if(vm.dataType == 'csv'){

        var separator = "";
        if(vm.csvSeparator == 'semicolon'){
          separator = ';';
        } else if(vm.csvSeparator == 'comma'){
          separator = ',';
        } else if(vm.csvSeparator == 'tab'){
          separator = '\t';
        } else if(vm.csvSeparator == 'space'){
          separator = ' ';
        } else if(vm.csvSeparator == 'custom'){
          separator = vm.csvSeparatorCustom;
        }

        var options = {
          file: vm.fileInfo,
          separator: separator,
          textQualifier: vm.csvTextQualifier,
          firstLineHeader: vm.csvFirstLineHeader
        };

        return prepareTableService.getCsvData(options)
          .then(loadData)
          .then(stopLoading)
          .catch(stopLoading);
      }
      if(vm.dataType == 'raw'){
        return prepareTableService.getRawData(vm.fileInfo)
          .then(loadData)
          .then(stopLoading)
          .catch(stopLoading);
      }
    };

    activate();

    function activate(){

      $(":file").filestyle();

    }
  }
})();
