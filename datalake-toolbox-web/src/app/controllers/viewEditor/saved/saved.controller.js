module.exports = {
  controller: SavedController,
  controllerAs: 'save',
  template: require('./saved.html')
};


/** @ngInject */
function SavedController($uibModalInstance, viewName){
  var vm = this;
  vm.viewName = viewName;
  vm.ok = function(){
    $uibModalInstance.close();
  };
}
