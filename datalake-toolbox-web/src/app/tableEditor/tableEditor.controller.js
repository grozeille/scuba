(function() {
  'use strict';

  angular
    .module('datalakeToolbox')
    .controller('TableEditorController', TableEditorController);

  /** @ngInject */
  function TableEditorController($timeout, $log, $location, $filter, $scope) {
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
    /*$scope.$watch("tableEditor.fileInfo", function( newValue, oldValue ) {
      $log.info(vm.fileInfo.name);
    }, true);*/
    $scope.$watch(function(scope){
      return(vm.fileInfo);
    }, function( newValue, oldValue ) {
      if(angular.isUndefined(newValue.name)){
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

    };


    vm.getData = function(){
      vm.isLoading = true;
      return preparationService.getData(vm.maxRows).then(function(data){
        if(data != null){
          vm.gridSampleOptions.data = data.data;
        }
        vm.isLoading = false;
      }).catch(function(error) {
        vm.isLoading = false;
      });
    };

    vm.cancelGetData = function(){
      preparationService.cancelGetData("user cancel");
    };

    activate();

    function activate(){

      $(":file").filestyle();

    }
  }
})();
