require('./catalog.css');

module.exports = {
  controller: CatalogController,
  controllerAs: 'catalog',
  template: require('./catalog.html')
};

/** @ngInject */
function CatalogController($timeout, $log, $location, $filter, $sce, hiveService) {
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
      for(var i = 0; i < vm.tables.length; i++){
        vm.tables[i].comment = $sce.trustAsHtml(vm.tables[i].comment);
      }
    });

  }
};
