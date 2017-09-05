require('./profile.css');

module.exports = {
  controller: ProfileController,
  controllerAs: 'profile',
  template: require('./profile.html')
};

/** @ngInject */
function ProfileController($log, userService, projectService) {
  var vm = this;

  vm.alerts = [];
  vm.currentProfile = {};

  vm.refresh = function() {
    userService.getCurrent().then(function(request) {
      vm.currentProfile = request.data;
      if(vm.currentProfile.lastProject !== null) {

      }
    })
    .catch(function(error) {
      vm.alerts.push({msg: 'Unable to load current profile .', type: 'danger'});
      throw error;
    });
  };

  function activate() {
    vm.refresh();
  }

  activate();
}
