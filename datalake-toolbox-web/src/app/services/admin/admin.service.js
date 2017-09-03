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

  var service = {
    setupFirstAdmin: setupFirstAdmin
  };

  return service;
}
