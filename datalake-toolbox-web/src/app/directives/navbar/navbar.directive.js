module.exports = navbar;

/** @ngInject */
function navbar($state) {
  var directive = {
    restrict: 'E',
    template: require('./navbar.html'),
    controller: function($scope) {
      var vm = this;
      vm.user = '';
      vm.authenticated = false;

      $scope.menu = [{
        name: 'Data Catalog',
        state: 'catalog'
      }, {
        name: 'DataSets',
        state: 'dataset'
      }/* , {
        name: 'Cubes',
        state: ''
      } */];
    }
  };

  return directive;
}
