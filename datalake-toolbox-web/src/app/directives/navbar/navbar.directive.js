module.exports = navbar;

/** @ngInject */
function navbar() {
  var directive = {
    restrict: 'E',
    template: require('./navbar.html'),
    controller: function($scope) {
      var vm = this;
      vm.user = '';
      vm.authenticated = false;

      $scope.menu = {
        left: [{
          name: 'DataSets',
          state: 'dataset'
        }/* , {
          name: 'Cubes',
          state: ''
        } */],
        right: [{
          name: 'Admin',
          state: 'admin'
        }, {
          name: 'Profile',
          state: 'profile'
        }]
      };
    }
  };

  return directive;
}
