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
    vm.csvSeparator = "comma";
    vm.csvSeparatorCustom = "";
    vm.csvTextQualifier = "doublequote";
    vm.csvFirstLineHeader = "true";
    vm.fileInfo = {};
    vm.excelSource = "";
    vm.excelSheets = [];
    vm.excelFirstLineHeader = "true";

    vm.jsTags = {
      edit: true,
      texts: {
        inputPlaceHolder: "Type text here"
      },
    };

    vm.gridOptions = {
      enableSorting: false,
      enableColumnMenus: false,
      enableColumnResizing: true,
      appScopeProvider: vm,
      columnDefs: [],
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

    function getSeparator(){

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

      return separator;
    }

    function getTextQualifier(){
      var textQualifier = "";
      if(vm.csvTextQualifier == 'doublequote'){
        textQualifier = '"';
      } else if(vm.csvTextQualifier == 'simplequote'){
        textQualifier = '\'';
      }
      return textQualifier;
    }


    vm.getData = function(){
      vm.isLoading = true;
      vm.gridOptions.columnDefs = [];
      vm.gridOptions.data = [ ];


      var loadData = function(data){
        if(data != null){

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

        var separator = getSeparator();

        var textQualifier = getTextQualifier();

        var options = {
          file: vm.fileInfo,
          separator: separator,
          textQualifier: textQualifier,
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

    vm.save = function(){

      var options = {
        file: vm.fileInfo,
        name: vm.name,
        description: vm.description,
        format: vm.dataType
      };

      if(vm.dataType == 'csv'){
        options.csvOptions = {
          separator: getSeparator(),
          textQualifier: getTextQualifier(),
          firstLineHeader: vm.csvFirstLineHeader
        };
      }
      else if(vm.dataType == 'excel'){
        options.excelOptions = {
          sheet: vm.excelSource,
          firstLineHeader: vm.excelFirstLineHeader
        };
      }

      return prepareTableService.save(options)
          .then(function(){
            $location.path( "/catalog" );
          });
    };

    activate();

    function activate(){

      $(":file").filestyle();

    }
  }
})();
