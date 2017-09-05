module.exports = {
  controller: ChooseDataSetTypeController,
  controllerAs: 'chooseDataSetType',
  template: require('./chooseDataSetType.html')
};

/** @ngInject */
function ChooseDataSetTypeController($uibModalInstance, $log, $state, preparationService) {
  var vm = this;

  vm.wrangleDataSet = function() {
    preparationService.loadView().then(function() {
      $state.go('datasetEditor');
    });
    $uibModalInstance.close();
  };

  vm.fileDataSet = function() {
    // $state.go('customFileDataSet({databaseAndTable: \'\'})');
    $state.go('customFileDataSet');
    $uibModalInstance.close();
  };

  vm.cancel = function() {
    $log.info('Modal dismissed at: ' + new Date());
    $uibModalInstance.dismiss();
  };
}
