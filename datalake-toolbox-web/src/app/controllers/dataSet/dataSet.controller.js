require('./dataset.css');

module.exports = {
  controller: DatasetController,
  controllerAs: 'dataset',
  template: require('./dataset.html')
};

/** @ngInject */
function DatasetController($timeout, $log, $location, $filter, $uibModal, $state, dataSetService, wranglingDataSetService) {
  var vm = this;
  vm.sourceFilter = '';

  vm.views = [];

  vm.createNewView = function() {
    vm.chooseDataSetType();
  };

  vm.openWranglingDataSet = function(database, table) {
    wranglingDataSetService.initDataSet(database, table).then(function() {
      $state.go('wranglingDataSet');
    });
  };

  vm.deleteDataSet = function(database, table) {
    dataSetService.getDataSet(database, table).then(function(data) {
      $uibModal.open({
        templateUrl: 'delete.html',
        controllerAs: 'delete',
        controller: function($uibModalInstance, dataSetName, parent) {
          var vm = this;
          vm.dataSetName = dataSetName;
          vm.ok = function() {
            dataSetService.deleteView(database, table).then(function() {
              $uibModalInstance.close();
              parent.loadAllDataSet();
            });
          };
          vm.cancel = function() {
            $uibModalInstance.dismiss('cancel');
          };
        },
        resolve: {
          dataSetName: function () {
            return database + '.' + table;
          },
          parent: function() {
            return vm;
          }
        }
      });
    });
  };

  vm.cloneDataSet = function(database, table) {
    // TODO test type of dataset
    wranglingDataSetService.cloneDataSet(database, table).then(function() {
      $state.go('wranglingDataSet');
    });
  };

  vm.loadAllDataSet = function() {
    dataSetService.getAllDataSet().then(function(data) {
      vm.dataSetList = data.content;
    });
  };

  var chooseDataSetTypeModal = require('./chooseDataSetType/chooseDataSetType.controller');

  vm.chooseDataSetType = function() {
    $uibModal.open(chooseDataSetTypeModal);
  };

  function activate() {
    vm.loadAllDataSet();
  }

  activate();
}
