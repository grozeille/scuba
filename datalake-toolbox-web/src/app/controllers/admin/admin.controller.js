require('./admin.css');

module.exports = {
  controller: AdminController,
  controllerAs: 'admin',
  template: require('./admin.html')
};

/** @ngInject */
function AdminController($log, $uibModal, adminService) {
  var vm = this;

  vm.alerts = [];
  vm.adminUsers = [];
  vm.newLogin = '';

  vm.closeAlert = function(index) {
    vm.alerts.splice(index, 1);
  };

  vm.refresh = function() {
    adminService.getAllAdmins()
      .then(function(response) {
        vm.adminUsers = response.data;
      })
      .catch(function(error) {
        vm.alerts.push({msg: 'Unable to get admins.', type: 'danger'});
        throw error;
      });
  };

  vm.remove = function(login) {
    $uibModal.open({
      templateUrl: 'delete.html',
      controllerAs: 'delete',
      controller: function($uibModalInstance, login, parent) {
        var vm = this;
        vm.login = login;
        vm.ok = function() {
          adminService.remove(login)
            .catch(function(error) {
              parent.alerts.push({msg: 'Unable to remove admin.', type: 'danger'});
              throw error;
            })
            .then(function() {
              parent.refresh();
            });
          $uibModalInstance.close();
        };
        vm.cancel = function() {
          $uibModalInstance.dismiss('cancel');
        };
      },
      resolve: {
        login: function () {
          return login;
        },
        parent: function() {
          return vm;
        }
      }
    });
  };

  vm.add = function() {
    adminService.add(vm.newLogin)
      .catch(function(error) {
        vm.alerts.push({msg: 'Unable to add new admin.', type: 'danger'});
        throw error;
      })
      .then(function() {
        vm.newLogin = '';
        vm.refresh();
      });
  };

  function activate() {
    vm.refresh();
  }

  activate();
}
