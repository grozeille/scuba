require('./profile.css');

module.exports = {
  controller: ProfileController,
  controllerAs: 'profile',
  template: require('./profile.html')
};

/** @ngInject */
function ProfileController($log) {
  var vm = this;

  function activate() {
  }

  activate();
}
