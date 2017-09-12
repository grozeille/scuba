module.exports = {
  controller: ChooseDataSetTypeController,
  controllerAs: 'chooseDataSetType',
  template: require('./chooseDataSetType.html')
};

/** @ngInject */
function ChooseDataSetTypeController($uibModalInstance, $log, $state, wranglingDataSetService) {
  var vm = this;

  vm.wrangleDataSet = function() {
    wranglingDataSetService.initDataSet().then(function() {
      $state.go('wranglingDataSet');
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
