module.exports = adminService;

/** @ngInject */
function adminService($log, $http, $location, $filter, $q, $rootScope) {
  var vm = this;
  vm.apiHost = $location.protocol() + '://' + $location.host() + ':' + $location.port() + '/api';

  vm.getServiceData = function(response) {
    return response.data;
  };

  vm.catchServiceException = function(error) {
    $log.error('XHR Failed.\n' + angular.toJson(error.data, true));
  };

  function setupFirstAdmin(token) {
    var url = vm.apiHost + '/admin/current-user';

    var request = {
      adminToken: token
    };

    return $http.post(url, request);
  }

  function getAllAdmins() {
    var url = vm.apiHost + '/admin';

    return $http.get(url);
  }

  function remove(login) {
    var url = vm.apiHost + '/admin/' + login;

    return $http.delete(url);
  }

  function add(login) {
    var url = vm.apiHost + '/admin/' + login;

    return $http.put(url);
  }

  var service = {
    setupFirstAdmin: setupFirstAdmin,
    getAllAdmins: getAllAdmins,
    add: add,
    remove: remove
  };

  return service;
}
