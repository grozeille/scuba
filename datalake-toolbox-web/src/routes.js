module.exports = routesConfig;

/** @ngInject */
function routesConfig($stateProvider, $urlRouterProvider) {
  $stateProvider
    .state('dataset', {
      url: '/dataset',
      component: 'datasetControllerComponent'
    })
    .state('datasetEditor', {
      url: '/dataset/editor',
      component: 'datasetEditorControllerComponent',
      params: {
        scrollTo: 'body'
      }
    })
    .state('chooseTable', {
      url: '/dataset/chooseTable',
      component: 'chooseTableControllerComponent'
    })
    .state('catalog', {
      url: '/catalog',
      component: 'catalogControllerComponent',
      params: {
        scrollTo: 'body'
      }
    })
    .state('tableEditor', {
      url: '/catalog/tableEditor/:database/:table',
      component: 'tableEditorControllerComponent',
      params: {
        scrollTo: 'body'
      }
    })
    .state('admin', {
      url: '/admin',
      component: 'adminControllerComponent'
    })
    .state('profile', {
      url: '/profile',
      component: 'profileControllerComponent'
    })
    .state('setup', {
      url: '/setup',
      component: 'setupControllerComponent'
    })
    ;

  $urlRouterProvider.otherwise('/dataset');
}
