module.exports = userService;

/** @ngInject */
function userService($log, $http, $location, $filter, $q, $rootScope) {
  var vm = this;
  vm.apiHost = $location.protocol() + '://' + $location.host() + ':' + $location.port() + '/api';

  function getCurrent() {
    var url = vm.apiHost + '/user/current';

    return $http.get(url);
  }

  function isCurrentAdmin() {
    var url = vm.apiHost + '/user/current/is-admin';

    return $http.get(url)
      .then(function(request) {
        return request.data;
      });
  }

  var service = {
    getCurrent: getCurrent,
    isCurrentAdmin: isCurrentAdmin
  };

  return service;
}
