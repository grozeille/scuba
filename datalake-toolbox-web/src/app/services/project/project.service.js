module.exports = projectService;

/** @ngInject */
function projectService($log, $http, $location, $filter, $q, $rootScope) {
  var vm = this;
  vm.apiHost = $location.protocol() + '://' + $location.host() + ':' + $location.port() + '/api';

  vm.getServiceData = function(response) {
    return response.data;
  };

  vm.catchServiceException = function(error) {
    $log.error('XHR Failed.\n' + angular.toJson(error.data, true));
  };

  function getAllProjects() {
    var url = vm.apiHost + '/project';

    return $http.get(url);
  }

  function remove(id) {
    var url = vm.apiHost + '/project/' + id;

    return $http.delete(url);
  }

  function add(project) {
    var url = vm.apiHost + '/project';

    return $http.post(url, project);
  }

  var service = {
    getAllProjects: getAllProjects,
    add: add,
    remove: remove
  };

  return service;
}
