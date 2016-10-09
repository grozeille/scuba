(function() {
  'use strict';

  angular
    .module('datalakeToolbox')
    .controller('CatalogController', CatalogController);

  /** @ngInject */
  function CatalogController($timeout, $log, $location, $filter, hiveService) {
    var vm = this;
    vm.sourceFilter = "";

    vm.tables = [];

    vm.delete = function(database, table){
    };

    vm.edit = function(database, table){
      $location.path( "/tableEditor/"+database+"/"+table );
    };


    activate();

    function activate(){
      hiveService.getTables().then(function(tables){
        vm.tables = tables;
      });

    }
  }
})();
