require('./viewDataSet.css');

module.exports = {
  controller: ViewDataSetController,
  controllerAs: 'viewDataSet',
  template: require('./viewDataSet.html')
};

/** @ngInject */
function ViewDataSetController($timeout, $log, $location, $filter, $q, $scope, $state, $window, viewDataSetService) {
  var vm = this;
  vm.isLoading = false;

  vm.alerts = [];

  vm.database = '';
  vm.name = '';
  vm.maxLinePreview = 5000;

  vm.gridOptions = {
    enableSorting: false,
    enableColumnMenus: false,
    enableColumnResizing: true,
    appScopeProvider: vm,
    columnDefs: [],
    data: [],
    onRegisterApi: function(gridApi) {
      vm.gridSampleApi = gridApi;
    }
  };

  vm.preview = function() {
    vm.isLoading = true;
    vm.gridOptions.columnDefs = [];
    vm.gridOptions.data = [];

    var stopLoading = function() {
      $log.info('Stop loading');
      vm.isLoading = false;
    };

    return viewDataSetService.getData(vm.maxLinePreview)
    .then(function(data) {
      $log.info('Refresh data');
      if(data !== null) {
        vm.gridOptions.data = data.data;
      }
    })
    .then(stopLoading)
    .catch(function(error) {
      vm.alerts.push({msg: 'Unable to preview data table.', type: 'danger'});
      $log.error(error);
      throw error;
    })
    .catch(stopLoading);
  };

  function activate() {
    var dataSet = viewDataSetService.getDataSet();
    vm.database = dataSet.database;
    vm.name = dataSet.name;
    vm.comment = dataSet.comment;

    vm.tags = [];
    for(var cpt = 0; cpt < dataSet.tags.length; cpt++) {
      vm.tags.push({text: dataSet.tags[cpt]});
    }
  }

  activate();
}
