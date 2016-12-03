module.exports = {
  controller: DeleteTableController,
  controllerAs: 'deleteTable',
  template: require('./deleteTable.html')
};


/** @ngInject */
function DeleteTableController($timeout, $log, $location, $filter, $uibModalInstance, $state, database, table, preparationService) {
 var vm = this;
 vm.table = preparationService.getTable(database, table);

 vm.ok = function(){
   $log.info('Modal OK at: ' + new Date());

   preparationService.removeTable(vm.table.database, vm.table.table);

   $uibModalInstance.close();
 };

 vm.cancel = function(){
   $log.info('Modal dismissed at: ' + new Date());
   $uibModalInstance.dismiss();
 };
}
