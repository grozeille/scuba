module.exports = {
  controller: DeleteCalculatedController,
  controllerAs: 'deleteCalculated',
  template: require('./deleteCalculated.html')
};


/** @ngInject */
function DeleteCalculatedController($timeout, $log, $location, $filter, $uibModalInstance, $state, columnName, preparationService) {
 var vm = this;
 vm.column = preparationService.getCalculatedColumn(columnName);

 vm.ok = function(){
   $log.info('Modal OK at: ' + new Date());

   preparationService.removeCalculatedColumn(vm.column.name);

   $uibModalInstance.close();
 };

 vm.cancel = function(){
   $log.info('Modal dismissed at: ' + new Date());
   $uibModalInstance.dismiss();
 };
}
