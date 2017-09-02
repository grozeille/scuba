require('./dataset.css');

module.exports = {
  controller: DatasetController,
  controllerAs: 'dataset',
  template: require('./dataset.html')
};

/** @ngInject */
function DatasetController($timeout, $log, $location, $filter, $uibModal, $state, preparationService) {
  var vm = this;
  vm.sourceFilter = '';

  vm.views = [];

  vm.createNewView = function() {
    vm.chooseDataSetType();
  };

  vm.loadView = function(id) {
    preparationService.loadView(id).then(function() {
      $state.go('datasetEditor');
    });
  };

  vm.deleteView = function(id) {
    preparationService.loadView(id);
    var viewName = preparationService.getName();
    preparationService.loadView();

    $uibModal.open({
      templateUrl: 'delete.html',
      controllerAs: 'delete',
      controller: function($uibModalInstance, viewName, parent) {
        var vm = this;
        vm.viewName = viewName;
        vm.ok = function() {
          preparationService.deleteView(id).then(function() {
            $uibModalInstance.close();
            parent.loadViews();
          });
        };
        vm.cancel = function() {
          $uibModalInstance.dismiss('cancel');
        };
      },
      resolve: {
        viewName: function () {
          return viewName;
        },
        parent: function() {
          return vm;
        }
      }
    });
  };

  vm.cloneView = function(id) {
    preparationService.cloneView(id).then(function() {
      $state.go('datasetEditor');
    });
  };

  vm.loadViews = function() {
    preparationService.getViews().then(function(data) {
      vm.views = data.content;
    });
  };

  var chooseDataSetTypeModal = require('./chooseDataSetType/chooseDataSetType.controller');

  vm.chooseDataSetType = function() {
    $uibModal.open(chooseDataSetTypeModal);
  };

  function activate() {
    vm.loadViews();
  }

  activate();
}
