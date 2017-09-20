require('./dataset.css');

module.exports = {
  controller: DatasetController,
  controllerAs: 'dataset',
  template: require('./dataset.html')
};

/** @ngInject */
function DatasetController($timeout, $log, $location, $filter, $uibModal, $state, dataSetService, wranglingDataSetService, customFileDataSetService) {
  var vm = this;

  vm.sourceFilter = '';
  vm.dataSetList = [];

  function getDataSet(database, table) {
    var selectedDataSet = null;
    for(var cpt = 0; cpt < vm.dataSetList.length; cpt++) {
      if(vm.dataSetList[cpt].database === database && vm.dataSetList[cpt].table === table) {
        selectedDataSet = vm.dataSetList[cpt];
        break;
      }
    }
    return selectedDataSet;
  }

  vm.createNewDataSet = function() {
    vm.chooseDataSetType();
  };

  vm.editDataSet = function(database, table) {
    var selectedDataSet = getDataSet(database, table);

    if(selectedDataSet !== null) {
      selectedDataSet.editLoading = true;
      if(selectedDataSet.dataSetType === 'CustomFileDataSet') {
        customFileDataSetService.initDataSet(database, table).then(function() {
          $state.go('customFileDataSet');
        });
      }
      else if(selectedDataSet.dataSetType === 'WranglingDataSet') {
        wranglingDataSetService.initDataSet(database, table).then(function() {
          $state.go('wranglingDataSet');
        });
      }
    }
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
            dataSetService.deleteDataSet(database, table).then(function() {
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

  var cloneDataSetModal = require('./cloneDataSet/cloneDataSet.controller');

  vm.cloneDataSet = function(database, table) {
    var selectedDataSet = getDataSet(database, table);

    selectedDataSet.cloneLoading = true;

    cloneDataSetModal.resolve = {
      sourceDatabase: function() {
        return database;
      },
      sourceTable: function() {
        return table;
      },
      dataSetType: function() {
        return selectedDataSet.dataSetType;
      }
    };

    $uibModal.open(cloneDataSetModal);
  };

  vm.loadAllDataSet = function() {
    dataSetService.getAllDataSet().then(function(data) {
      vm.dataSetList = data.content;
      for(var cpt = 0; cpt < vm.dataSetList.length; cpt++) {
        vm.dataSetList[cpt].editLoading = false;
        vm.dataSetList[cpt].cloneLoading = false;
      }
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
