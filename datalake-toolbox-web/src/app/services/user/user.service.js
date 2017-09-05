module.exports = userService;

/** @ngInject */
function userService($log, $http, $location, $filter, $q, $rootScope) {
  var vm = this;
  vm.apiHost = $location.protocol() + '://' + $location.host() + ':' + $location.port() + '/api';

  function getCurrent() {
    var url = vm.apiHost + '/user/current';

    return $http.get(url);
  }

  function getMemberProjects() {
    var url = vm.apiHost + '/user/current/project';

    return $http.get(url);
  }

  function updateLastProject(projectId) {
    var url = vm.apiHost + '/user/current/last-project';

    var request = {
      projectId: projectId
    };

    return $http.post(url, request);
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
    isCurrentAdmin: isCurrentAdmin,
    getMemberProjects: getMemberProjects,
    updateLastProject: updateLastProject
  };

  return service;
}
