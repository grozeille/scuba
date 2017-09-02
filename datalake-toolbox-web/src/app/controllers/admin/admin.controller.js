require('./admin.css');

module.exports = {
  controller: AdminController,
  controllerAs: 'admin',
  template: require('./admin.html')
};

/** @ngInject */
function AdminController($log) {
  var vm = this;

  function activate() {
  }

  activate();
}
